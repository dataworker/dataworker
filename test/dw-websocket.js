QUnit.module("DataWorker (Streaming via Websockets)");

DataWorker.workerPool._src = "resources/dw-helper.test.js";

WebSocket.unexpected = function (msg) {
    assert.ok(false, msg);
};

QUnit.test("construct (webworker w/ authenticate)", function (assert) {
    assert.expect(3);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : { "asdfzxcv": '{"trigger":true,"msg":"authenticated"}' }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource   : "ws://websocket.test.com:8085",
        authenticate : "asdfzxcv",
        onTrigger    : function (msg) {
            assert.equal(msg, "authenticated", "authenticated");

            d.getColumns(function (columns) { assert.ok(true); }).finish(done);
        }
    });

    assert.ok(d instanceof DataWorker);
});

QUnit.test("construct (single-threaded w/ authenticate)", function (assert) {
    assert.expect(3);

    var done = assert.async();

    WebSocket.expectedSource  = "ws://websocket.test.com:8085";
    WebSocket.expectedReplies = {
        "asdfzxcv": function () { assert.ok(true, "authenticate received"); }
    };

    var d = new DataWorker({
        datasource        : "ws://websocket.test.com:8085",
        authenticate      : "asdfzxcv",
        forceSingleThread : true
    });

    assert.ok(d instanceof DataWorker);

    d.getColumns(function (columns) { assert.ok(true) }).finish(done);
});

QUnit.test("construct (webworker w/ request)", function (assert) {
    assert.expect(3);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource       : "ws://websocket.test.com:8085",
        request          : "REQUEST_DATASET",
        onReceiveColumns: function () {
            d.getExpectedNumberOfRecords(function (expected) {
                assert.equal(expected, 4, "expected number of records");
            });
        },
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                assert.deepEqual(columns, {
                    column_a: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_a",
                        name      : "column_a",
                        isVisible : true,
                        index     : 0
                    },
                    column_b: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_b",
                        name      : "column_b",
                        isVisible : true,
                        index     : 1
                    },
                    column_c: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_c",
                        name      : "column_c",
                        isVisible : true,
                        index     : 2
                    }
                });

                assert.deepEqual(records, [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ],
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]);

                d.finish(done);
            });
        }
    });
});

QUnit.test("construct (single-threaded w/ request)", function (assert) {
    assert.expect(3);

    var done = assert.async();

    WebSocket.expectedSource = "ws://websocket.test.com:8085";
    WebSocket.expectedReplies = {
        "REQUEST_DATASET": [
            {
                expectedNumRows : 4,
                columns         : [ "column_a", "column_b", "column_c" ]
            },
            {
                rows: [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ]
                ]
            },
            {
                rows: [
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]
            }
        ].map(function (reply) { return JSON.stringify(reply); })
    };

    var d = new DataWorker({
        forceSingleThread: true,
        datasource       : "ws://websocket.test.com:8085",
        request          : "REQUEST_DATASET",
        onReceiveColumns: function () {
            d.getExpectedNumberOfRecords(function (expected) {
                assert.equal(expected, 4, "expected number of records");
            });
        },
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                assert.deepEqual(columns, {
                    column_a: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_a",
                        name      : "column_a",
                        isVisible : true,
                        index     : 0
                    },
                    column_b: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_b",
                        name      : "column_b",
                        isVisible : true,
                        index     : 1
                    },
                    column_c: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_c",
                        name      : "column_c",
                        isVisible : true,
                        index     : 2
                    }
                });

                assert.deepEqual(records, [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ],
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]);

                d.finish(done);
            });
        }
    });
});

QUnit.test("construct (complex datasource)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : { "asdfzxcv": '{"trigger":true,"msg":"authenticated"}' }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource: {
            source       : "ws://websocket.test.com:8085",
            authenticate : "asdfzxcv"
        },
        onTrigger: function (msg) {
            assert.equal(msg, "authenticated", "authenticated");

            d.getColumns(function (columns) { assert.ok(true); }).finish(done);
        }
    });
});

QUnit.test("construct (fallback to websocket)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : { "asdfzxcv": '{"trigger":true,"msg":"authenticated"}' }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource: [
            {
                source       : srcPath + "foo/bar"
            },
            {
                source       : "ws://websocket.test.com:8085",
                authenticate : "asdfzxcv"
            }
        ],
        onTrigger: function (msg) {
            assert.equal(msg, "authenticated", "authenticated");

            d.finish(done);
        }
    });
});

QUnit.test("requestDataset", function (assert) {
    assert.expect(4);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET_1": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); }),
                "REQUEST_DATASET_2": [
                    {
                        expectedNumRows : 3,
                        columns         : [ "column_d", "column_e", "column_f" ]
                    },
                    {
                        rows: [
                            [ "trilogy",    "shakespeare", "soul"   ]
                        ]
                    },
                    {
                        rows: [
                            [ "motorcycle", "tire",        "tissue" ],
                            [ "almonds",    "kodaline",    "body"   ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var datasetNum = 0;

    var d = new DataWorker({
        datasource: "ws://websocket.test.com:8085",
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                if (datasetNum++ === 0) {
                    assert.deepEqual(columns, {
                        column_a: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_a",
                            name      : "column_a",
                            isVisible : true,
                            index     : 0
                        },
                        column_b: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_b",
                            name      : "column_b",
                            isVisible : true,
                            index     : 1
                        },
                        column_c: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_c",
                            name      : "column_c",
                            isVisible : true,
                            index     : 2
                        }
                    });

                    assert.deepEqual(records, [
                        [ "apple",      "violin",    "music" ],
                        [ "cat",        "tissue",      "dog" ],
                        [ "banana",      "piano",      "gum" ],
                        [ "gummy",       "power",     "star" ]
                    ]);

                    d.requestDataset("REQUEST_DATASET_2");
                } else {
                    assert.deepEqual(columns, {
                        column_d: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_d",
                            name      : "column_d",
                            isVisible : true,
                            index     : 0
                        },
                        column_e: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_e",
                            name      : "column_e",
                            isVisible : true,
                            index     : 1
                        },
                        column_f: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_f",
                            name      : "column_f",
                            isVisible : true,
                            index     : 2
                        }
                    });

                    assert.deepEqual(records, [
                        [ "trilogy",    "shakespeare", "soul"   ],
                        [ "motorcycle", "tire",        "tissue" ],
                        [ "almonds",    "kodaline",    "body"   ]
                    ]);

                    d.finish(done);
                }
            });
        }
    }).requestDataset("REQUEST_DATASET_1");
});

QUnit.test("requestDataset w/ cancelRequests", function (assert) {
    assert.expect(4);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET_1": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); }),
                "REQUEST_DATASET_2": [
                    {
                        expectedNumRows : 3,
                        columns         : [ "column_d", "column_e", "column_f" ]
                    },
                    {
                        rows: [
                            [ "trilogy",    "shakespeare", "soul"   ]
                        ]
                    },
                    {
                        rows: [
                            [ "motorcycle", "tire",        "tissue" ],
                            [ "almonds",    "kodaline",    "body"   ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); }),
                "CANCEL": JSON.stringify({ trigger: true, msg: "CANCEL_ACK" })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var datasetNum = 0;

    var d = new DataWorker({
        datasource: "ws://websocket.test.com:8085",
        cancelRequestsCmd: "CANCEL",
        cancelRequestsAck: "CANCEL_ACK",
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                if (datasetNum++ === 0) {
                    assert.deepEqual(columns, {
                        column_a: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_a",
                            name      : "column_a",
                            isVisible : true,
                            index     : 0
                        },
                        column_b: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_b",
                            name      : "column_b",
                            isVisible : true,
                            index     : 1
                        },
                        column_c: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_c",
                            name      : "column_c",
                            isVisible : true,
                            index     : 2
                        }
                    });

                    assert.deepEqual(records, [
                        [ "apple",      "violin",    "music" ],
                        [ "cat",        "tissue",      "dog" ],
                        [ "banana",      "piano",      "gum" ],
                        [ "gummy",       "power",     "star" ]
                    ]);

                    d.requestDataset("REQUEST_DATASET_2");
                } else {
                    assert.deepEqual(columns, {
                        column_d: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_d",
                            name      : "column_d",
                            isVisible : true,
                            index     : 0
                        },
                        column_e: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_e",
                            name      : "column_e",
                            isVisible : true,
                            index     : 1
                        },
                        column_f: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_f",
                            name      : "column_f",
                            isVisible : true,
                            index     : 2
                        }
                    });

                    assert.deepEqual(records, [
                        [ "trilogy",    "shakespeare", "soul"   ],
                        [ "motorcycle", "tire",        "tissue" ],
                        [ "almonds",    "kodaline",    "body"   ]
                    ]);

                    d.finish(done);
                }
            });
        }
    }).requestDataset("REQUEST_DATASET_1");
});

QUnit.test("requestDatasetForAppend", function (assert) {
    assert.expect(4);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET_1": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); }),
                "REQUEST_DATASET_2": [
                    {
                        expectedNumRows : 3,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "trilogy",    "shakespeare", "soul"   ]
                        ]
                    },
                    {
                        rows: [
                            [ "motorcycle", "tire",        "tissue" ],
                            [ "almonds",    "kodaline",    "body"   ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); }),
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var datasetNum = 0;

    var d = new DataWorker({
        datasource: "ws://websocket.test.com:8085",
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                if (datasetNum++ === 0) {
                    assert.deepEqual(columns, {
                        column_a: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_a",
                            name      : "column_a",
                            isVisible : true,
                            index     : 0
                        },
                        column_b: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_b",
                            name      : "column_b",
                            isVisible : true,
                            index     : 1
                        },
                        column_c: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_c",
                            name      : "column_c",
                            isVisible : true,
                            index     : 2
                        }
                    });

                    assert.deepEqual(records, [
                        [ "apple",      "violin",    "music" ],
                        [ "cat",        "tissue",      "dog" ],
                        [ "banana",      "piano",      "gum" ],
                        [ "gummy",       "power",     "star" ]
                    ]);

                    d.requestDatasetForAppend("REQUEST_DATASET_2");
                } else {
                    assert.deepEqual(columns, {
                        column_a: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_a",
                            name      : "column_a",
                            isVisible : true,
                            index     : 0
                        },
                        column_b: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_b",
                            name      : "column_b",
                            isVisible : true,
                            index     : 1
                        },
                        column_c: {
                            sortType  : "alpha",
                            aggType   : "max",
                            title     : "column_c",
                            name      : "column_c",
                            isVisible : true,
                            index     : 2
                        }
                    });

                    assert.deepEqual(records, [
                        [ "apple",      "violin",      "music"  ],
                        [ "cat",        "tissue",      "dog"    ],
                        [ "banana",     "piano",       "gum"    ],
                        [ "gummy",      "power",       "star"   ],
                        [ "trilogy",    "shakespeare", "soul"   ],
                        [ "motorcycle", "tire",        "tissue" ],
                        [ "almonds",    "kodaline",    "body"   ]
                    ]);

                    d.finish(done);
                }
            });
        }
    }).requestDataset("REQUEST_DATASET_1");
});

QUnit.test("clearDataset", function (assert) {
    assert.expect(4);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource        : "ws://websocket.test.com:8085",
        request           : "REQUEST_DATASET",
        onAllRowsReceived : function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                assert.deepEqual(columns, {
                    column_a: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_a",
                        name      : "column_a",
                        isVisible : true,
                        index     : 0
                    },
                    column_b: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_b",
                        name      : "column_b",
                        isVisible : true,
                        index     : 1
                    },
                    column_c: {
                        sortType  : "alpha",
                        aggType   : "max",
                        title     : "column_c",
                        name      : "column_c",
                        isVisible : true,
                        index     : 2
                    }
                });

                assert.deepEqual(records, [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ],
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]);
            });

            d.clearDataset();

            d.getAllColumnsAndAllRecords(function (columns, records) {
                assert.deepEqual(columns, {});
                assert.deepEqual(records, []);
            }).finish(done);
        }
    });
});

QUnit.test("cancelOngoingRequests", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "CANCEL": JSON.stringify({ trigger: true, msg: "CANCEL_ACK" })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource: "ws://websocket.test.com:8085",
        cancelRequestsCmd: "CANCEL",
        cancelRequestsAck: "CANCEL_ACK",
    }).cancelOngoingRequests().getColumns(function () {
        assert.ok(true, "reached this point");
        d.finish(done);
    });
});

QUnit.test("onReceiveRows callback", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var rowsReceived = 0;

    var d = new DataWorker({
        datasource        : "ws://websocket.test.com:8085",
        request           : "REQUEST_DATASET",
        onReceiveRows     : function (numRows) { rowsReceived++; },
        onAllRowsReceived : function () {
            rowsReceived++; // onAllRowsReceived gets fired *instead of* onReceiveRows

            assert.equal(rowsReceived, 2);

            d.finish(done);
        }
    });
});

QUnit.test("onSocketClose", function (assert) {
    assert.expect(2);

    var done = assert.async();

    WebSocket.expectedSource  = "ws://websocket.test.com:8085";
    WebSocket.expectedReplies = {
        "CLOSE": function () { assert.ok(true, "CLOSE command received"); }
    };

    var d = new DataWorker({
        datasource        : "ws://websocket.test.com:8085",
        forceSingleThread : true,
        onSocketClose     : "CLOSE"
    });

    assert.ok(d instanceof DataWorker);

    d.getColumns(function (columns) { assert.ok(true) }).finish(done);
});

QUnit.test("attempt reconnect after disconnect", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            interruptAfter  : 100,
            expectedSource  : "ws://websocket.test.com:8085",
            expectedReplies : {
                "REQUEST_DATASET": [
                    {
                        expectedNumRows : 4,
                        columns         : [ "column_a", "column_b", "column_c" ]
                    },
                    {
                        rows: [
                            [ "apple",      "violin",    "music" ],
                            [ "cat",        "tissue",      "dog" ]
                        ]
                    },
                    {
                        rows: [
                            [ "banana",      "piano",      "gum" ],
                            [ "gummy",       "power",     "star" ]
                        ]
                    }
                ].map(function (reply) { return JSON.stringify(reply); })
            }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource             : "ws://websocket.test.com:8085",
        shouldAttemptReconnect : true,
        onAllRowsReceived      : function () {
            assert.ok(true, 'all rows recieved after interrupted connection');
            d.finish(done);
        }
    });

    setTimeout(function () {
        d.requestDataset("REQUEST_DATASET");
    }, 200);
});

QUnit.test("mock websocket displays unexpected errors correctly in web worker", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var worker = DataWorker.workerPool.getWorker();
    worker.postMessage({
        meta: {
            expectedSource  : "ws://qwer",
            expectedReplies : { "asdffdsa": '{"trigger":true,"msg":"authenticated"}' }
        }
    });
    DataWorker.workerPool.reclaim(worker);

    var d = new DataWorker({
        datasource: "ws://zxcv",
        authenticate: "asdffdsa",
        onError: function (error) {
            assert.equal(error, "Unexpected:\n\tExpected: ws://qwer\n\tGot: ws://zxcv");

            d.finish(done);
        }
    });
});
