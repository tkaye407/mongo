/**
 * Tests administrative sharding operations and map-reduce work or fail as expected, when key-based
 * authentication is used
 *
 * This test is labeled resource intensive because its total io_write is 30MB compared to a median
 * of 5MB across all sharding tests in wiredTiger.
 * @tags: [resource_intensive]
 */
(function() {
    'use strict';

    var shardTest = new ShardingTest({
        name: "traffic_recording",
        mongos: 1,
        shards: 0,
    });

    // Get the mongos and its admin database
    var mongos = shardTest.s;
    var admin = mongos.getDB("admin");

    // Running the serverStatus command should return running is false
    var res = admin.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    var trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], false);

    // Without setting the trafficRecordingDirectory the startRecordingTraffic command should fail
    res = admin.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording directory not set");

    // Restart mongos with the trafficRecordingDirectory paramter set
    const dbPath = MongoRunner.toRealDir("$dataDir/traffic_recording/");
    mkdir(dbPath);
    print("Restart mongos with different auth options");
    shardTest.restartMongos(0,
                            {restart: true, setParameter: "trafficRecordingDirectory=" + dbPath});

    // Get the mongos and its admin database
    mongos = shardTest.s0;
    admin = mongos.getDB("admin");

    // Running the serverStatus command should return running is false
    res = admin.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], false);

    // After setting the trafficRecordingDirectory the startRecordingTraffic command should succeed
    res = admin.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, true);

    // Running the serverStatus command should return the relevant information
    res = admin.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], true);

    // Assert that the current file size is growing
    res = admin.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    var trafficStats2 = res["trafficRecording"];
    assert.eq(trafficStats2["running"], true);
    assert(trafficStats2["currentFileSize"] > trafficStats["currentFileSize"]);

    // Running the stopRecordingTraffic command should succeed
    res = admin.runCommand({'stopRecordingTraffic': 1});
    assert.eq(res.ok, true);

    // Running the stopRecordingTraffic command again should fail
    res = admin.runCommand({'stopRecordingTraffic': 1});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording not active");

    // Running the serverStatus command should return running is false
    var res = admin.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    var trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], false);

    // Shutdown the shard test
    shardTest.stop();
})();
