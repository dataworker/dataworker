QUnit.module("DataWorker (AJAX Data)");

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

QUnit.test("construct (webworker)", function (assert) {
    assert.expect(7);

    var done = assert.async();

    var d = new DataWorker({
        datasource: srcPath + "resources/simple-dataset.json"
    }).requestDataset();

    assert.ok(d instanceof DataWorker);

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            assert.equal(Object.keys(columns).length, 3);
            assert.equal(columns.column_a.index, 0);
            assert.equal(columns.column_b.index, 1);
            assert.equal(columns.column_c.index, 2);
        }).getRows(function (rows) {
            assert.deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ]);
        }).getSummaryRows(function (rows) {
            assert.deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);
        }).finish(done);
    });
});

QUnit.test("construct (single-thread)", function (assert) {
    assert.expect(7);

    var done = assert.async();

    var d = new DataWorker({
        forceSingleThread: true,
        datasource: srcPath + "resources/simple-dataset.json"
    }).requestDataset();

    assert.ok(d instanceof DataWorker);

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            assert.equal(Object.keys(columns).length, 3);
            assert.equal(columns.column_a.index, 0);
            assert.equal(columns.column_b.index, 1);
            assert.equal(columns.column_c.index, 2);
        }).getRows(function (rows) {
            assert.deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ]);
        }).getSummaryRows(function (rows) {
            assert.deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);
        }).finish(done);
    });
});

QUnit.test("AJAX as a fallback for when websocket fails", function (assert) {
    assert.expect(7);

    var done = assert.async();

    var d = new DataWorker({
        datasource: [
            "ws://localhost/foo/bar",
            srcPath + "resources/simple-dataset.json"
        ]
    }).requestDataset();

    assert.ok(d instanceof DataWorker);

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            assert.equal(Object.keys(columns).length, 3);
            assert.equal(columns.column_a.index, 0);
            assert.equal(columns.column_b.index, 1);
            assert.equal(columns.column_c.index, 2);
        }).getRows(function (rows) {
            assert.deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ]);
        }).getSummaryRows(function (rows) {
            assert.deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);
        }).finish(done);
    });
});

QUnit.test("Can use onReceiveColumns with AJAX", function (assert) {
    assert.expect(7);

    var done = assert.async();

    var d = new DataWorker({
        datasource: srcPath + "resources/simple-dataset.json"
    }).requestDataset();

    assert.ok(d instanceof DataWorker);

    d.onReceiveColumns(function () {
        d.getColumns(function (columns) {
            assert.equal(Object.keys(columns).length, 3);
            assert.equal(columns.column_a.index, 0);
            assert.equal(columns.column_b.index, 1);
            assert.equal(columns.column_c.index, 2);
        });
    }).onAllRowsReceived(function () {
        d.getRows(function (rows) {
            assert.deepEqual(rows, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ "gummy",       "power",     "star" ]
            ]);
        }).getSummaryRows(function (rows) {
            assert.deepEqual(rows, [
                [ "acbg", "vtpp", "mdgs" ]
            ]);
        }).finish(done);
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

QUnit.test("Streaming ajax", function (assert) {
    assert.expect(7);

    var done = assert.async();

    var d = new DataWorker({
        datasource: srcPath + "resources/streaming-dataset-300-rows.json"
    }).requestDataset();

    d.onAllRowsReceived(function () {
        d.getColumns(function (columns) {
            assert.equal(Object.keys(columns).length, 2);

            assert.equal(columns.numbers.index, 0);
            assert.equal(columns.numbers.sortType, "num");

            assert.equal(columns.words.index, 1);
            assert.equal(columns.words.sortType, "alpha");
        }).getNumberOfRecords(function (numRecords) {
            assert.equal(numRecords, 300);
        }).getSummaryRows(function (rows) {
            assert.deepEqual(rows, [
                [ 12345, "Total" ],
                [ 54321, "latoT" ]
            ]);
        }).finish(done);
    });
});

QUnit.test("large, incoming datasets do not crash dataworker", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var d = new DataWorker({
        datasource: srcPath + "resources/million-row-dataset.json"
    }).requestDataset().onAllRowsReceived(function () {
        d.getRows(function (records) {
            assert.equal(records.length, 1000000, "has a million records");
        }).finish(done);
    });
});
