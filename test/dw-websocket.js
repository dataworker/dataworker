module("DataWorker (Streaming via Websockets)");

DataWorker.workerPool._src = "resources/dw-helper.test.js";

WebSocket.unexpected = function (msg) {
    ok(false, msg);
};

asyncTest("construct (webworker w/ authenticate)", function () {
    expect(2);

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
            equal(msg, "authenticated", "authenticated");
        }
    });

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) { start(); }).finish();
});

asyncTest("construct (single-threaded w/ authenticate)", function () {
    expect(2);

    WebSocket.expectedSource  = "ws://websocket.test.com:8085";
    WebSocket.expectedReplies = {
        "asdfzxcv": function () { ok(true, "authenticate received"); }
    };

    var d = new DataWorker({
        datasource        : "ws://websocket.test.com:8085",
        authenticate      : "asdfzxcv",
        forceSingleThread : true
    });

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) { start(); }).finish();
});

asyncTest("construct (webworker w/ request)", function () {
    expect(3);

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
                equal(expected, 4, "expected number of records");
            });
        },
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                deepEqual(columns, {
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

                deepEqual(records, [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ],
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]);

                start();
            });

            d.finish();
        }
    });
});

asyncTest("construct (single-threaded w/ request)", function () {
    expect(3);

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
                equal(expected, 4, "expected number of records");
            });
        },
        onAllRowsReceived: function () {
            d.getAllColumnsAndAllRecords(function (columns, records) {
                deepEqual(columns, {
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

                deepEqual(records, [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ],
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]);

                start();
            });

            d.finish();
        }
    });
});

asyncTest("construct (complex datasource)", function () {
    expect(1);

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
            equal(msg, "authenticated", "authenticated");
        }
    });

    d.getColumns(function (columns) { start(); }).finish();
});

asyncTest("construct (fallback to websocket)", function () {
    expect(1);

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
            equal(msg, "authenticated", "authenticated");

            d.finish();
            start();
        }
    });
});

asyncTest("requestDataset", function () {
    expect(4);

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
                    deepEqual(columns, {
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

                    deepEqual(records, [
                        [ "apple",      "violin",    "music" ],
                        [ "cat",        "tissue",      "dog" ],
                        [ "banana",      "piano",      "gum" ],
                        [ "gummy",       "power",     "star" ]
                    ]);

                    d.requestDataset("REQUEST_DATASET_2");
                } else {
                    deepEqual(columns, {
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

                    deepEqual(records, [
                        [ "trilogy",    "shakespeare", "soul"   ],
                        [ "motorcycle", "tire",        "tissue" ],
                        [ "almonds",    "kodaline",    "body"   ]
                    ]);

                    d.finish();
                    start();
                }
            });
        }
    }).requestDataset("REQUEST_DATASET_1");
});

asyncTest("requestDataset w/ cancelRequests", function () {
    expect(4);

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
                    deepEqual(columns, {
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

                    deepEqual(records, [
                        [ "apple",      "violin",    "music" ],
                        [ "cat",        "tissue",      "dog" ],
                        [ "banana",      "piano",      "gum" ],
                        [ "gummy",       "power",     "star" ]
                    ]);

                    d.requestDataset("REQUEST_DATASET_2");
                } else {
                    deepEqual(columns, {
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

                    deepEqual(records, [
                        [ "trilogy",    "shakespeare", "soul"   ],
                        [ "motorcycle", "tire",        "tissue" ],
                        [ "almonds",    "kodaline",    "body"   ]
                    ]);

                    d.finish();
                    start();
                }
            });
        }
    }).requestDataset("REQUEST_DATASET_1");
});

asyncTest("requestDatasetForAppend", function () {
    expect(4);

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
                    deepEqual(columns, {
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

                    deepEqual(records, [
                        [ "apple",      "violin",    "music" ],
                        [ "cat",        "tissue",      "dog" ],
                        [ "banana",      "piano",      "gum" ],
                        [ "gummy",       "power",     "star" ]
                    ]);

                    d.requestDatasetForAppend("REQUEST_DATASET_2");
                } else {
                    deepEqual(columns, {
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

                    deepEqual(records, [
                        [ "apple",      "violin",      "music"  ],
                        [ "cat",        "tissue",      "dog"    ],
                        [ "banana",     "piano",       "gum"    ],
                        [ "gummy",      "power",       "star"   ],
                        [ "trilogy",    "shakespeare", "soul"   ],
                        [ "motorcycle", "tire",        "tissue" ],
                        [ "almonds",    "kodaline",    "body"   ]
                    ]);

                    d.finish();
                    start();
                }
            });
        }
    }).requestDataset("REQUEST_DATASET_1");
});

asyncTest("clearDataset", function () {
    expect(4);

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
                deepEqual(columns, {
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

                deepEqual(records, [
                    [ "apple",      "violin",    "music" ],
                    [ "cat",        "tissue",      "dog" ],
                    [ "banana",      "piano",      "gum" ],
                    [ "gummy",       "power",     "star" ]
                ]);
            });

            d.clearDataset();

            d.getAllColumnsAndAllRecords(function (columns, records) {
                deepEqual(columns, {});
                deepEqual(records, []);

                d.finish();
                start();
            });
        }
    });
});

asyncTest("cancelOngoingRequests", function () {
    expect(1);

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
        ok(true, "reached this point");
        d.finish();
        start();
    });
});

asyncTest("onReceiveRows callback", function () {
    expect(1);

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

            equal(rowsReceived, 2);

            start();
            d.finish();
        }
    });
});

asyncTest("onSocketClose", function () {
    expect(1);

    WebSocket.expectedSource  = "ws://websocket.test.com:8085";
    WebSocket.expectedReplies = {
        "CLOSE": function () { ok(true, "CLOSE command received"); }
    };

    var d = new DataWorker({
        datasource        : "ws://websocket.test.com:8085",
        forceSingleThread : true,
        onSocketClose     : "CLOSE"
    });

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) { start(); }).finish();
});
