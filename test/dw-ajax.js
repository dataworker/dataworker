module("DataWorker (AJAX Data)");

/* resources/simple-dataset.json:

{
    "columns": [ "column_a", "column_b", "column_c" ],
    "rows": [
        [ "apple",  "violin", "music" ],
        [ "cat",    "tissue", "dog"   ],
        [ "banana", "piano",  "gum"   ],
        [ "gummy",  "power",  "star"  ]
    ],
    "summaryRows": [
        [ "acbg",   "vtpp",   "mdgs"  ]
    ],
}

*/

asyncTest("construct (webworker)", function () {
    expect(7);

    var d = new DataWorker({
        forceSingleThread: true,
        datasource: srcPath + "resources/simple-dataset.json"
    }).requestDataset();

    ok(d instanceof DataWorker);

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            equal(Object.keys(columns).length, 3);
            equal(columns["column_a"].index, 0);
            equal(columns["column_b"].index, 1);
            equal(columns["column_c"].index, 2);
        }).getRows(function (rows) {
            deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ]);
        }).getSummaryRows(function (rows) {
            deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);

            start();
        }).finish();
    });
});

asyncTest("construct (single-thread)", function () {
    expect(7);

    var d = new DataWorker({
        forceSingleThread: true,
        datasource: srcPath + "resources/simple-dataset.json"
    }).requestDataset();

    ok(d instanceof DataWorker);

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            equal(Object.keys(columns).length, 3);
            equal(columns["column_a"].index, 0);
            equal(columns["column_b"].index, 1);
            equal(columns["column_c"].index, 2);
        }).getRows(function (rows) {
            deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ])
        }).getSummaryRows(function (rows) {
            deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);

            start();
        }).finish();
    });
});

asyncTest("AJAX as a fallback for when websocket fails", function () {
    expect(7);

    var workerSource = DataWorker.workerPool._src;
    DataWorker.workerPool._src = "../src/dw-helper.js";

    var d = new DataWorker({
        datasource: [
            "ws://localhost/foo/bar",
            srcPath + "resources/simple-dataset.json"
        ]
    }).requestDataset();

    ok(d instanceof DataWorker);

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            equal(Object.keys(columns).length, 3);
            equal(columns["column_a"].index, 0);
            equal(columns["column_b"].index, 1);
            equal(columns["column_c"].index, 2);
        }).getRows(function (rows) {
            deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ])
        }).getSummaryRows(function (rows) {
            deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);

            DataWorker.workerPool._src = workerSource;
            start();
        }).finish();
    });
});

asyncTest("Can use onReceiveColumns with AJAX", function () {
    expect(7);

    var d = new DataWorker({
        datasource: srcPath + "resources/simple-dataset.json"
    }).requestDataset();

    ok(d instanceof DataWorker);

    d.onReceiveColumns(function () {
        d.getColumns(function (columns) {
            equal(Object.keys(columns).length, 3);
            equal(columns["column_a"].index, 0);
            equal(columns["column_b"].index, 1);
            equal(columns["column_c"].index, 2);
        });
    }).onAllRowsReceived(function () {
        d.getRows(function (rows) {
            deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ])
        }).getSummaryRows(function (rows) {
            deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);

            start();
        }).finish();
    });
});

/* resources/streaming-dataset-300-rows.json:

{
    "columns": [
        { "name": "numbers", "sortType": "num" },
        { "name": "words", "sortType": "alpha" }
    ],
    "expectedNumRows": 300
}

{
    "summaryRows": [
        [ 12345, "Total" ],
        [ 54321, "latoT" ]
    ]
}

The remaining 100 lines were generated by running:
    outputRowsForStreaming(generateRandomDataset([ "num", "alpha" ], 300), 3);

*/

asyncTest("Streaming ajax", function () {
    expect(7);

    var d = new DataWorker({
        datasource: srcPath + "resources/streaming-dataset-300-rows.json"
    }).requestDataset();

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            equal(Object.keys(columns).length, 2);

            equal(columns.numbers.index, 0);
            equal(columns.numbers.sortType, "num");

            equal(columns.words.index, 1);
            equal(columns.words.sortType, "alpha");
        }).getNumberOfRecords(function (numRecords) {
            equal(numRecords, 300);
        }).getSummaryRows(function (rows) {
            deepEqual(rows, [
                [ 12345, "Total" ],
                [ 54321, "latoT" ]
            ]);

            start();
        }).finish();
    });
});
