module("DataWorker (AJAX Data)");

/* resources/simple-dataset.json:

{
    "columns": [ "column_a", "column_b", "column_c" ],
    "rows": [
        [ "apple",  "violin", "music" ],
        [ "cat",    "tissue", "dog"   ],
        [ "banana", "piano",  "gum"   ],
        [ "gummy",  "power",  "star"  ]
    ]
}

*/

asyncTest("construct (webworker)", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

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

            start();
        }).finish();
    });
});

asyncTest("construct (single-thread)", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

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

            start();
        }).finish();
    });
});

asyncTest("AJAX as a fallback for when websocket fails", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

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

            start();
        }).finish();
    });
});

asyncTest("Can use onReceiveColumns with AJAX", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

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

The remaining 100 lines were generated by running:
    outputRowsForStreaming(generateRandomDataset([ "num", "alpha" ], 300), 3);

*/

asyncTest("Streaming ajax", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

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

            start();
        }).finish();
    });
});
