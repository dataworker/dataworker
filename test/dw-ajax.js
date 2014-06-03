module("DataWorker (AJAX Data)");

var srcPath = (function () {
    var scripts = document.getElementsByTagName("script"),
        srcFile = scripts[scripts.length - 1].src;

    return srcFile.replace(/((?:https?|file):\/\/)?.*?(\/(.*\/)?).*/, "$1$2");
})();

function getRandomWord() {
    var length = parseInt(Math.random() * 30),
        word = "", i;

    for (i = 0; i < length; i++) {
        word += String.fromCharCode(65 + parseInt(Math.random() * 61));
    }

    return word;
}

function generateRandomDataset(columnTypes, numRows) {
    var rows = [], i;

    for (i = 0; i < numRows; i++) {
        rows.push(columnTypes.reduce(function (row, type) {
            row.push(type === "num" ? Math.random() * 1000 : getRandomWord());
            return row;
        }, []));
    }

    return rows;
}

function outputRowsForStreaming(dataset, rowsPerLine) {
    var dataset = generateRandomDataset([ "num", "alpha" ], 300),
        output  = [];

    while (dataset.length) {
        output.push(JSON.stringify({ rows: dataset.splice(0, rowsPerLine) }));
    }

    console.log(output.join("\n"));
}

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
