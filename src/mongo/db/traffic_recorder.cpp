/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/traffic_recorder.h"

#include <boost/filesystem/operations.hpp>
#include <fstream>

#include "mongo/base/data_builder.h"
#include "mongo/base/data_type_terminated.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/service_context.h"
#include "mongo/rpc/factory.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/producer_consumer_queue.h"

namespace mongo {

namespace {

constexpr auto kDefaultTrafficRecordingDirectory = ""_sd;

MONGO_EXPORT_STARTUP_SERVER_PARAMETER(trafficRecordingDirectory,
                                      std::string,
                                      kDefaultTrafficRecordingDirectory.toString())
    ->withValidator([](const std::string& newValue) {
        if (!boost::filesystem::is_directory(newValue)) {
            return Status(ErrorCodes::FileNotOpen,
                          str::stream() << "traffic recording directory \"" << newValue
                                        << "\" is not a directory.");
        }

        return Status::OK();
    });

}  // namespace


/**
 * The Recording class represents a single recording that the recorder is exposing.  It's made up of
 * a background thread which flushes records to disk, and helper methods to push to that thread,
 * expose stats, and stop the recording.
 */
class TrafficRecorder::Recording {
public:
    Recording(const StartRecordingTraffic& options)
        : _path(_getPath(options.getFilename().toString())), _maxLogSize(options.getMaxFileSize()) {

        MultiProducerSingleConsumerQueue<TrafficRecordingPacket, CostFunction>::Options
            queueOptions;
        queueOptions.maxQueueDepth = options.getBufferSize();
        _pcqPipe = MultiProducerSingleConsumerQueue<TrafficRecordingPacket, CostFunction>::Pipe(
            queueOptions);

        _trafficStats.setRunning(true);
        _trafficStats.setBufferSize(options.getBufferSize());
        _trafficStats.setRecordingFile(_path);
        _trafficStats.setMaxFileSize(_maxLogSize);
    }

    void run() {
        _thread = stdx::thread([ consumer = std::move(_pcqPipe.consumer), this ] {
            try {
                DataBuilder db;
                std::vector<TrafficRecordingPacket> storage;

                std::fstream out(_path,
                                 std::ios_base::binary | std::ios_base::trunc | std::ios_base::out);

                while (true) {
                    storage.clear();
                    consumer.popManyUpTo(1 << 24, std::back_inserter(storage));

                    for (const auto& packet : storage) {
                        db.clear();
                        Message toWrite = packet.message;

                        uassertStatusOK(db.writeAndAdvance<LittleEndian<uint32_t>>(0));
                        uassertStatusOK(db.writeAndAdvance<LittleEndian<uint64_t>>(packet.id));
                        uassertStatusOK(db.writeAndAdvance<Terminated<'\0', StringData>>(
                            StringData(packet.local)));
                        uassertStatusOK(db.writeAndAdvance<Terminated<'\0', StringData>>(
                            StringData(packet.remote)));
                        uassertStatusOK(db.writeAndAdvance<LittleEndian<uint64_t>>(
                            packet.now.toMillisSinceEpoch()));
                        uassertStatusOK(db.writeAndAdvance<LittleEndian<uint64_t>>(packet.order));

                        auto size = db.size() + toWrite.size();
                        uassertStatusOK(db.getCursor().write<LittleEndian<uint32_t>>(size));

                        {
                            stdx::lock_guard<stdx::mutex> lk(_mutex);
                            _written += size;
                        }

                        uassert(ErrorCodes::LogWriteFailed,
                                "hit maximum log size",
                                _written < _maxLogSize);

                        out.write(db.getCursor().data(), db.size());
                        out.write(toWrite.buf(), toWrite.size());
                    }
                }
            } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueConsumed>&) {
                // Close naturally
            } catch (...) {
                auto status = exceptionToStatus();

                stdx::lock_guard<stdx::mutex> lk(_mutex);
                _result = status;
            }
        });
    }

    /**
     * pushRecord returns false if the queue was full.  This is ultimately fatal to the recording
     */
    bool pushRecord(const transport::SessionHandle& ts,
                    Date_t now,
                    const uint64_t order,
                    const Message& message) {
        try {
            // If we pushed our packet, we're good
            if (_pcqPipe.producer.tryPush({ts->id(),
                                           ts->local().toString(),
                                           ts->remote().toString(),
                                           now,
                                           order,
                                           message})) {
                return true;
            }

            // Otherwise we begin the process of failing the recording
            _pcqPipe.producer.close();

            stdx::lock_guard<stdx::mutex> lk(_mutex);

            // If we the result was otherwise okay, mark it as failed due to the queue blocking.  If
            // it failed for another reason, don't overwrite that.
            if (_result.isOK()) {
                _result =
                    Status(ErrorCodes::ProducerConsumerQueueWouldBlock, "queue would have blocked");
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
        }

        return false;
    }

    Status shutdown() {
        stdx::unique_lock<stdx::mutex> lk(_mutex);

        if (!_inShutdown) {
            _inShutdown = true;
            lk.unlock();

            _pcqPipe.producer.close();
            _thread.join();

            lk.lock();
        }

        return _result;
    }

    BSONObj getStats() {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _trafficStats.setBufferedBytes(_pcqPipe.controller.getStats().queueDepth);
        _trafficStats.setCurrentFileSize(_written);
        return _trafficStats.toBSON();
    }

    AtomicWord<uint64_t> order{0};

private:
    struct TrafficRecordingPacket {
        const uint64_t id;
        const std::string local;
        const std::string remote;
        const Date_t now;
        const uint64_t order;
        const Message message;
    };

    struct CostFunction {
        size_t operator()(const TrafficRecordingPacket& packet) const {
            return packet.message.size();
        }
    };

    static std::string _getPath(const std::string& filename) {
        uassert(ErrorCodes::BadValue,
                "Traffic recording filename must not be empty",
                !filename.empty());

        if (trafficRecordingDirectory.back() == '/') {
            trafficRecordingDirectory.pop_back();
        }
        auto parentPath = boost::filesystem::path(trafficRecordingDirectory);
        auto path = parentPath / filename;

        uassert(ErrorCodes::BadValue,
                "Traffic recording filename must be a simple filename",
                path.parent_path() == parentPath);

        return path.string();
    }

    const std::string _path;
    const size_t _maxLogSize;

    MultiProducerSingleConsumerQueue<TrafficRecordingPacket, CostFunction>::Pipe _pcqPipe;
    stdx::thread _thread;

    stdx::mutex _mutex;
    bool _inShutdown = false;
    TrafficRecorderStats _trafficStats;
    size_t _written = 0;
    Status _result = Status::OK();
};

namespace {
const auto getTrafficRecorder = ServiceContext::declareDecoration<TrafficRecorder>();
}

TrafficRecorder& TrafficRecorder::get(ServiceContext* svc) {
    return getTrafficRecorder(svc);
}

void TrafficRecorder::start(const StartRecordingTraffic& options) {
    uassert(ErrorCodes::BadValue,
            "Traffic recording directory not set",
            !trafficRecordingDirectory.empty());

    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        uassert(ErrorCodes::BadValue, "Traffic recording already active", !_recording);

        _recording = std::make_shared<Recording>(options);
        _recording->run();
    }

    _shouldRecord.store(true);
}

void TrafficRecorder::stop() {
    _shouldRecord.store(false);

    auto recording = [&] {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        uassert(ErrorCodes::BadValue, "Traffic recording not active", _recording);

        return std::move(_recording);
    }();

    uassertStatusOK(recording->shutdown());
}

void TrafficRecorder::observe(const transport::SessionHandle& ts,
                              Date_t now,
                              const Message& message) {
    if (!_shouldRecord.load()) {
        return;
    }

    auto recording = [&] {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        return _recording;
    }();

    // If we don't have an active recording, bail
    if (!recording) {
        return;
    }

    // Try to record the message
    if (recording->pushRecord(ts, now, recording->order.addAndFetch(1), message)) {
        return;
    }

    // We couldn't queue
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    // If the recording isn't the one we have in hand bail (its been ended, or a new one has
    // been created
    if (_recording != recording) {
        return;
    }

    // We couldn't queue and it's still our recording.  No one else should try to queue
    _shouldRecord.store(false);
}

class TrafficRecorder::TrafficRecorderSSS : public ServerStatusSection {
public:
    TrafficRecorderSSS() : ServerStatusSection("trafficRecording") {}

    bool includeByDefault() const override {
        return true;
    }

    BSONObj generateSection(OperationContext* opCtx,
                            const BSONElement& configElement) const override {
        auto& recorder = TrafficRecorder::get(opCtx->getServiceContext());

        if (!recorder._shouldRecord.load()) {
            return BSON("running" << false);
        }

        auto recording = [&] {
            stdx::lock_guard<stdx::mutex> lk(recorder._mutex);
            return recorder._recording;
        }();

        return recording->getStats();
    }
} trafficRecorderStats;

}  // namespace mongo
