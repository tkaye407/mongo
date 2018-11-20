// tests for the traffic_recording commands.
(function() {
    // Without setting the trafficRecordingDirectory the startRecordingTraffic command should fail
    var baseName = "jstests_traffic_recording";
    var m = MongoRunner.runMongod({auth: ""});
    var db = m.getDB("admin");

    db.createUser({user: "admin", pwd: "pass", roles: jsTest.adminUserRoles});
    db.auth("admin", "pass");

    var res = db.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording directory not set");
    MongoRunner.stopMongod(m, null, {user: 'admin', pwd: 'password'});

    // After setting the trafficRecordingDirectory the startRecordingTraffic command should succeed
    const dbPath = MongoRunner.toRealDir("$dataDir/traffic_recording/");
    mkdir(dbPath);
    var opts = {auth: "", setParameter: "trafficRecordingDirectory=" + dbPath};
    m = MongoRunner.runMongod(opts);
    db = m.getDB("admin");
    db.createUser({user: "admin", pwd: "pass", roles: jsTest.adminUserRoles});
    db.auth("admin", "pass");
    res = db.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, true);

    // Running the command again should fail
    res = db.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording already active");

    // Running the serverStatus command should return the relevant information
    res = db.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    var trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], true);

    // Assert that the current file size is growing
    res = db.runCommand({"serverStatus": 1});
    res = db.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    var trafficStats2 = res["trafficRecording"];
    assert.eq(trafficStats2["running"], true);
    assert(trafficStats2["currentFileSize"] >= trafficStats["currentFileSize"]);

    // Running the stopRecordingTraffic command should succeed
    res = db.runCommand({'stopRecordingTraffic': 1});
    assert.eq(res.ok, true);

    // Running the stopRecordingTraffic command again should fail
    res = db.runCommand({'stopRecordingTraffic': 1});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording not active");

    // Running the serverStatus command should return running is false
    res = db.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    var trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], false);

    // Shutdown Mongod
    MongoRunner.stopMongod(m, null, {user: 'admin', pwd: 'password'});
})();
