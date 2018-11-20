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

#include <exception>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <io.h>
#endif

#include "mongo/base/data_cursor.h"
#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_endian.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/base/data_view.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/traffic_reader.h"
#include "mongo/rpc/factory.h"
#include "mongo/rpc/message.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/time_support.h"

namespace {
// Taken from src/mongo/gotools/mongoreplay/util.go
// Time.Unix() returns the number of seconds from the unix epoch but time's
// underlying struct stores 'sec' as the number of seconds elapsed since
// January 1, year 1 00:00:00 UTC (In the Proleptic Gregorian Calendar)
// This calculation allows for conversion between the internal representation
// and the UTC representation.
const long long unixToInternal =
    static_cast<long long>(1969 * 365 + 1969 / 4 - 1969 / 100 + 1969 / 400) * 86400;
}

namespace mongo {

class Done {};

void readBytes(size_t toRead, char* buf, int& fd) {
    while (toRead) {
#ifdef _WIN32
        auto r = _read(fd, buf, toRead);
#else
        auto r = ::read(fd, buf, toRead);
#endif

        if (r == -1) {
            if (errno == EINTR) {
                continue;
            }

            throw std::runtime_error("failed to read bytes");
        } else if (r == 0) {
            throw Done();
        }

        buf += r;
        toRead -= r;
    }
}

TrafficReaderPacket readPacket(char* buf, int& fd) {
    readBytes(4, buf, fd);
    auto len = ConstDataView(buf).read<LittleEndian<uint32_t>>();

    if (len > (1 << 26)) {
        throw std::runtime_error("packet too large");
    }

    readBytes(len - 4, buf + 4, fd);

    ConstDataRangeCursor cdr(buf, buf + len);

    // Read the packet
    uassertStatusOK(cdr.skip<LittleEndian<uint32_t>>());
    uint64_t id = uassertStatusOK(cdr.readAndAdvance<LittleEndian<uint64_t>>());
    StringData local = uassertStatusOK(cdr.readAndAdvance<Terminated<'\0', StringData>>());
    StringData remote = uassertStatusOK(cdr.readAndAdvance<Terminated<'\0', StringData>>());
    uint64_t date = uassertStatusOK(cdr.readAndAdvance<LittleEndian<uint64_t>>());
    uint64_t order = uassertStatusOK(cdr.readAndAdvance<LittleEndian<uint64_t>>());
    MsgData::ConstView message(cdr.data());

    return {id, local, remote, Date_t::fromMillisSinceEpoch(date), order, message};
}

BSONObj getBSONObjFromPacket(TrafficReaderPacket& packet, bool withOpType) {
    // Builder will hold the mongoreplay representation of this current packet
    BSONObjBuilder builder;

    // RawOp Field
    BSONObjBuilder rawop;

    // Add the header fields to rawOp
    BSONObjBuilder header;
    header.append("messagelength", static_cast<int32_t>(packet.message.getLen()));
    header.append("requestid", static_cast<int32_t>(packet.message.getId()));
    header.append("responseto", static_cast<int32_t>(packet.message.getResponseToMsgId()));
    header.append("opcode", static_cast<int32_t>(packet.message.getNetworkOp()));
    rawop.append("header", header.obj());

    // Add the binary reprentation of the entire message for rawop.body
    // auto buf = SharedBuffer::allocate(packet.message.getLen());
    // std::memcpy(buf.get(), packet.message.view2ptr(), packet.message.getLen());
    // rawop.appendBinData("body", packet.message.getLen(), BinDataGeneral, buf.get());
    rawop.appendBinData("body", packet.message.getLen(), BinDataGeneral, packet.message.view2ptr());


    // Add RawOp to Builder
    builder.append("rawop", rawop.obj());

    // The seen field represents the time that the operation took place
    // Trying to re-create the way mongoreplay does this
    BSONObjBuilder seen;
    seen.append("sec",
                static_cast<int64_t>((packet.date.toMillisSinceEpoch() / 1000) + unixToInternal));
    seen.append("nsec", static_cast<int32_t>(packet.order));
    builder.append("seen", seen.obj());

    // Figure out which is the src endpoint as opposed to the dest endpoint
    auto localInd = packet.local.rfind(':');
    auto remoteInd = packet.remote.rfind(':');
    if (localInd != std::string::npos && remoteInd != std::string::npos) {
        auto local = packet.local.substr(localInd + 1);
        auto remote = packet.remote.substr(remoteInd + 1);
        if (packet.message.getResponseToMsgId()) {
            builder.append("srcendpoint", local);
            builder.append("destendpoint", remote);
        } else {
            builder.append("srcendpoint", remote);
            builder.append("destendpoint", local);
        }
    }

    // Fill out the remaining fields
    builder.append("order", static_cast<int64_t>(packet.order));
    builder.append("seenconnectionnum", static_cast<int64_t>(packet.id));
    builder.append("playedconnectionnum", static_cast<int64_t>(0));
    builder.append("generation", static_cast<int32_t>(0));

    if (withOpType) {
        if (packet.message.getNetworkOp() == dbMsg) {
            Message message;
            message.setData(dbMsg, packet.message.data(), packet.message.dataLen());

            auto opMsg = rpc::opMsgRequestFromAnyProtocol(message);
            builder.append("opType", opMsg.getCommandName());
        } else {
            builder.append("opType", "legacy");
        }
    }

    return builder.obj();
}

BSONArray mongoGetRecordedDocuments(const std::string& inputFile) {
    BSONArrayBuilder builder{};

    std::ifstream infile(inputFile.c_str());
    if (!infile.good()) {
        std::cout << "Error: Specified file does not exist (" << inputFile.c_str() << ")"
                  << std::endl;
        return builder.arr();
    }

// Open the connection to the input file
#ifdef _WIN32
    auto inputFd = open(inputFile.c_str(), O_RDONLY | O_BINARY);
#else
    auto inputFd = open(inputFile.c_str(), O_RDONLY);
#endif

    auto buf = SharedBuffer::allocate(1 << 26);
    try {
        while (true) {
            // Parse the buffer into a packet
            auto packet = readPacket(buf.get(), inputFd);

            // Dump the object into the output file
            auto obj = getBSONObjFromPacket(packet, true);

            builder.append(obj);
        }
    } catch (const Done&) {
    }

    return builder.arr();
}

int mongoTrafficReaderMain(int inputFd, std::ofstream& outputStream) {
    // Document expected by mongoreplay
    BSONObjBuilder opts{};
    opts.append("playbackfileversion", 1);
    opts.append("driveropsfiltered", false);
    auto optsObj = opts.obj();
    outputStream.write(optsObj.objdata(), optsObj.objsize());

    auto buf = SharedBuffer::allocate(1 << 26);
    try {
        while (true) {
            // Parse the buffer into a packet
            auto packet = readPacket(buf.get(), inputFd);

            // Dump the object into the output file
            auto obj = getBSONObjFromPacket(packet, false);
            outputStream.write(obj.objdata(), obj.objsize());
        }
    } catch (const Done&) {
    }

    return 0;
}

}  // namespace mongo
