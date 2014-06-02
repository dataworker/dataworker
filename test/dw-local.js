/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 *                                                                           *
 * Tests for DataWorker                                                      *
 *                                                                           *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

module("DataWorker (Local Data)");

asyncTest("construct (simple columns)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
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

        start();
    }).finish();
});

asyncTest("construct (force single-threaded execution)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    dataset.forceSingleThread = true;

    var d = new DataWorker(dataset);

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
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

        start();
    }).finish();
});

asyncTest("construct (complex columns)", function () {
    expect(2);

    var dataset = [
        [
            {
                name: "column_a",
                aggType: "max",
                sortType: "alpha",
                title: "Column A"
            },
            {
                name: "column_b",
                aggType: "max",
                sortType: "alpha",
                title: "Column B"
            },
            {
                name: "column_c",
                aggType: "min",
                sortType: "alpha",
                title: "Column C"
            }
        ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
        deepEqual(columns, {
            column_a: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "Column A",
                name      : "column_a",
                isVisible : true,
                index     : 0
            },
            column_b: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "Column B",
                name      : "column_b",
                isVisible : true,
                index     : 1
            },
            column_c: {
                sortType  : "alpha",
                aggType   : "min",
                title     : "Column C",
                name      : "column_c",
                isVisible : true,
                index     : 2
            }
        });

        start();
    }).finish();
});

asyncTest("apply filter (simple, unrestricted)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/apple/);

    d.getDataset(function(result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (simple, column-restricted, single column, found)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/apple/, "column_a").getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (simple, column-restricted, single column, not found)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/apple/, "column_b").getDataset(function (result) {
        deepEqual(result, []);
        start();
    }).finish();
});


asyncTest("apply filter (simple, column-restricted, multi-column, found)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/apple/, "column_a", "column_c")
     .sort("column_a")
     .getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power", "apple"  ]
        ]);
        start();
     }).finish();
});

asyncTest("apply filter (simple, column-restricted, multi-column, not found)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/piano/, "column_a", "column_c").getDataset(function (result) {
        deepEqual(result, []);
        start();
    }).finish();
});

asyncTest("apply filter (complex, single)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(
        {
            column : "column_a",
            regex  : "^apple$"
        }
    ).getDataset(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (complex, multi)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "cat",         "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(
        {
            column : "column_a",
            regex  : "^cat$"
        },
        {
            column : "column_c",
            regex  : "^dog|gum$"
        }
    ).getDataset(function (result) {
        deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "cat",         "piano",      "gum" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (complex, ignores column not found)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "cat",         "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(
        {
            column : "column_a",
            regex  : "^cat$"
        },
        {
            column : "column_c",
            regex  : "^dog|gum$"
        },
        {
            column : "column_d",
            regex  : "^nothing$"
        }
    ).getDataset(function (result) {
        deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "cat",         "piano",      "gum" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (simple, multiple filters)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/m/).getDataset(function (result) {
        deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "star"  ]
        ]);
    }).applyFilter(/e/).getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power",  "star"  ]
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (complex, multiple filters)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "cat",         "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(
        {
            column : "column_a",
            regex  : "^cat$"
        },
        {
            column : "column_c",
            regex  : "^dog|gum$"
        },
        {
            column : "column_d",
            regex  : "^nothing$"
        }
    ).applyFilter(
        {
            column : "column_b",
            regex  : "e",
        }
    ).getDataset(function (result) {
        deepEqual(result, [
            [ "cat", "tissue", "dog" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (complex then simple, multiple filters)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "cat",         "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(
        {
            column : "column_a",
            regex  : "^cat$"
        },
        {
            column : "column_c",
            regex  : "^dog|gum$"
        },
        {
            column : "column_d",
            regex  : "^nothing$"
        }
    ).applyFilter(/e/).getDataset(function (result) {
        deepEqual(result, [
            [ "cat", "tissue", "dog" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (simple then complex, multiple filters)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "cat",         "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/e/).applyFilter(
        {
            column : "column_a",
            regex  : "^cat$"
        },
        {
            column : "column_c",
            regex  : "^dog|gum$"
        },
        {
            column : "column_d",
            regex  : "^nothing$"
        }
    ).getDataset(function (result) {
        deepEqual(result, [
            [ "cat", "tissue", "dog" ],
        ]);
        start();
    }).finish();
});

asyncTest("apply filter (complex, single filter, multiple columns)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(
        {
            columns : [ "column_a", "column_c" ],
            regex   : "^apple$"
        }
    ).getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power",  "apple" ]
        ]);
        start();
    }).finish();
});

asyncTest("clear filter", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/apple/).clearFilters();

    d.getDataset(function(result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
        start();
    }).finish();
});

asyncTest("filter (hard)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.filter(/apple/);

    d.getDataset(function(result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("limit", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyLimit(2).getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue", "dog"   ]
        ]);
        start();
    }).finish();
});

asyncTest("limit (hard)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.limit(2).getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue", "dog"   ]
        ]);
        start();
    }).finish();
});

asyncTest("remove columns", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.removeColumns("column_b", "column_c")
     .getColumnsAndRecords(function (columns, rows) {
        deepEqual(columns, {
            column_a : {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_a",
                name      : "column_a",
                isVisible : true,
                index     : 0
            }
        });
        deepEqual(rows, [
            [ "apple"  ],
            [ "cat"    ],
            [ "banana" ],
            [ "gummy"  ]
        ]);

        start();
    }).finish();
});

asyncTest("sort (alpha)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.sort("column_b").getDataset(function (result) {
        deepEqual(result, [
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "apple" ],
            [ "cat",    "tissue", "dog"   ],
            [ "apple",  "violin", "music" ]
        ]);
        start();
    }).finish();
});

// FIXME: localeCompare doesn"t seem to work properly in Web Worker threads.
//asyncTest("sort (locale-considering alpha)", function () {
//    expect(1);

//    var dataset = [
//        [
//            {
//                name: "column_a",
//                aggType: "max",
//                sortType: "localeAlpha",
//                title: "Column A"
//            },
//            {
//                name: "column_b",
//                aggType: "max",
//                sortType: "alpha",
//                title: "Column B"
//            },
//            {
//                name: "column_c",
//                aggType: "min",
//                sortType: "alpha",
//                title: "Column C"
//            }
//        ],

//        [ "rip",        "violin",    "music" ],
//        [ "résumé",     "tissue",      "dog" ],
//        [ "gummy",       "power",     "star" ],
//        [ "banana",      "piano",      "gum" ]
//    ];

//    var d = new DataWorker(dataset);

//    d.sort("column_a").getDataset(function (result) {
//        deepEqual(result, [
//            [ "banana",      "piano",      "gum" ],
//            [ "gummy",       "power",     "star" ],
//            [ "résumé",     "tissue",      "dog" ],
//            [ "rip",        "violin",    "music" ]
//        ]);

//        start();
//    }).finish();
//});

asyncTest("sort (reverse alpha)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.sort("-column_b").getDataset(function (result) {
        deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "gummy",  "power",  "apple" ],
            [ "banana", "piano",  "gum"   ]
        ]);
        start();
    }).finish();
});

asyncTest("sort (num)", function () {
    expect(1);

    var dataset = [
        [
            {
                name: "column_a",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_b",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_c",
                sortType: "num",
                aggType: "max"
            },
        ],

        [ "apple",      "violin",          8 ],
        [ "cat",        "tissue",         85 ],
        [ "banana",      "piano",         45 ],
        [ "gummy",       "power",         82 ]
    ];

    var d = new DataWorker(dataset);
    d.sort("column_c").getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin",  8 ],
            [ "banana", "piano", 45 ],
            [ "gummy",  "power", 82 ],
            [ "cat",   "tissue", 85 ]
        ]);
        start();
    }).finish();
});

asyncTest("sort (num w/ decimals)", function () {
    expect(1);

    var dataset = [
        [
            {
                name: "column_a",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_b",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_c",
                sortType: "num",
                aggType: "max"
            },
        ],

        [ "apple",      "violin",          "8.0" ],
        [ "cat",        "tissue",         "85.0" ],
        [ "banana",      "piano",         "45.0" ],
        [ "gummy",       "power",         "82.0" ]
    ];

    var d = new DataWorker(dataset);
    d.sort("column_c").getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin",  "8.0" ],
            [ "banana", "piano", "45.0" ],
            [ "gummy",  "power", "82.0" ],
            [ "cat",   "tissue", "85.0" ]
        ]);
        start();
    }).finish();
});

asyncTest("sort (reverse num)", function () {
    expect(1);
    var dataset = [
        [
            {
                name: "column_a",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_b",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_c",
                sortType: "num",
                aggType: "max"
            },
        ],

        [ "apple",      "violin",          8 ],
        [ "cat",        "tissue",         85 ],
        [ "banana",      "piano",         45 ],
        [ "gummy",       "power",         82 ]
    ];

    var d = new DataWorker(dataset);
    d.sort("-column_c").getDataset(function (result) {
        deepEqual(result, [
            [ "cat",   "tissue", 85 ],
            [ "gummy",  "power", 82 ],
            [ "banana", "piano", 45 ],
            [ "apple", "violin",  8 ]
        ]);
        start();
    }).finish();
});

asyncTest("sort (multi-column)", function () {
    expect(1);

    var dataset = [
        [
            {
                name: "column_a",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_b",
                sortType: "alpha",
                aggType: "max"
            },
            {
                name: "column_c",
                sortType: "num",
                aggType: "max"
            },
        ],

        [ "apple",      "violin",          5 ],
        [ "cat",        "tissue",         85 ],
        [ "banana",      "piano",         45 ],
        [ "cat",         "power",         98 ]
    ];

    var d = new DataWorker(dataset);
    d.sort("column_a", "column_c").getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin",  5 ],
            [ "banana", "piano", 45 ],
            [ "cat",   "tissue", 85 ],
            [ "cat",    "power", 98 ]
        ]);
        start();
    }).finish();
});

asyncTest("paginate (set page)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);

    d.setPage(3).getPage(function (result) {
        deepEqual(result, [
            [ "car",        "screen",    "phone" ],
            [ "sign",        "bagel",    "chips" ]
        ]);
        start();
    }).finish();
});

asyncTest("paginate (set 1st page)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);

    d.setPage(1).getPage(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
        start();
    }).finish();
});

asyncTest("paginate (set 0th page)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);

    d.setPage(0).getPage(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
        start();
    }).finish();
});

asyncTest("paginate (set negative page)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);

    d.setPage(-1).getPage(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
        start();
    }).finish();
});

asyncTest("paginate (next page)", function () {
    expect(4);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var page1, page2, page3, page4;

    var d = new DataWorker(dataset).paginate(2).render(function () {
        deepEqual(page1, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        deepEqual(page2, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ]
        ]);
        deepEqual(page3, [
            [ "car", "screen", "phone" ],
            [ "sign", "bagel", "chips" ]
        ]);
        deepEqual(page4, page3);

        start();
    });

    d.getNextPage(function (result) {
        page1 = result;
    }).getNextPage(function (result) {
        page2 = result;
    }).getNextPage(function (result) {
        page3 = result;
    }).getNextPage(function (result) {
        page4 = result;
    }).render().finish();
});

asyncTest("paginate (previous page)", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var page3, page2, page1, page0;

    var d = new DataWorker(dataset).paginate(2).setPage(3);

    d.getPreviousPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ]
        ]);
        equal(pageNum, 2);
    }).getPreviousPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        equal(pageNum, 1);
    }).getPreviousPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        equal(pageNum, 1);

        start();
    }).finish();
});

asyncTest("paginate (get current page)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2).setPage(2);
    d.getPage(function (page) {
        deepEqual(page, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ],
        ]);
        start();
    }).finish();
});

asyncTest("paginate (get specific page)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);
    var page = d.getPage(function (page) {
        deepEqual(page, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ],
        ]);
        start();
    }, 2).finish();
});

asyncTest("paginate (getPage passes rows and current page number to callback)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);
    var page = d.getPage(function (page, pageNum) {
        deepEqual(page, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ],
        ]);

        equal(pageNum, 2);
        start();
    }, 2).finish();
});

asyncTest("paginate (request after last page returns last page)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);
    var page = d.getPage(function (page, pageNum) {
        deepEqual(page, [
            [ "car",  "screen", "phone" ],
            [ "sign", "bagel",  "chips" ]
        ]);

        equal(pageNum, 3);
        start();
    }, 7).finish();
});

asyncTest("paginate (only returns visible rows)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);
    var page = d.applyFilter(/a/, "column_a").getPage(function (page, pageNum) {
        deepEqual(page, [
            [ "banana", "piano",  "gum"   ],
            [ "car",    "screen", "phone" ],
        ]);

        equal(pageNum, 2);
        start();
    }, 2).finish();
});

asyncTest("paginate (next page when already on last page keeps dataset on last page", function () {
    expect(8);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);

    d.getNextPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        equal(pageNum, 1);
    }).getNextPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ]
        ]);
        equal(pageNum, 2);
    }).getNextPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "car", "screen", "phone" ],
            [ "sign", "bagel", "chips" ]
        ]);
        equal(pageNum, 3);
    }).getNextPage(function (rows, pageNum) {
        deepEqual(rows, [
            [ "car", "screen", "phone" ],
            [ "sign", "bagel", "chips" ]
        ]);
        equal(pageNum, 3);

        start();
    }).finish();
});

asyncTest("paginate (get number of pages)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset).paginate(2);

    d.getNumberOfPages(function (numPages) {
        equal(numPages, 3);
        start();
    }).finish();
});

asyncTest("append", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_a", "column_b", "column_c" ],

        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset1);
    d.append(dataset2).getDataset(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",    "apple" ],
            [ "car",        "screen",    "phone" ],
            [ "sign",        "bagel",    "chips" ]
        ]);
        start();
    }).finish();
});

asyncTest("append DataWorker", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_a", "column_b", "column_c" ],

        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);
    d1.append(d2).getDataset(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",    "apple" ],
            [ "car",        "screen",    "phone" ],
            [ "sign",        "bagel",    "chips" ]
        ]);
        start();
    }).finish();
});

asyncTest("failed append (columns not the same)", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_a", "column_b", "column_d" ],

        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset1).onError(function (error) {
        equal(
            error,
            "Cannot append dataset (columns do not match):\n\t"
            + "column_a, column_b, column_c\n\t\t"
            + "VS\n\t"
            + "column_a, column_b, column_d"
        );

        start();
    });

    d.append(dataset2).finish();
});

asyncTest("failed append (different number of columns)", function () {
    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_a", "column_b" ],

        [ "gummy",       "power" ],
        [ "car",        "screen" ],
        [ "sign",        "bagel" ]
    ];

    var d = new DataWorker(dataset1).onError(function (error) {
        equal(
            error,
            "Cannot append dataset (columns do not match):\n\t"
            + "column_a, column_b, column_c\n\t\t"
            + "VS\n\t"
            + "column_a, column_b"
        );

        start();
    });

    d.append(dataset2).finish();
});

asyncTest("join (inner join on single field)", function () {
    expect(2);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_d", "column_e", "column_f" ],

        [ "banana",      "power",    "apple" ],
        [ "apple",      "screen",    "phone" ],
        [ "cat",         "bagel",    "chips" ],
        [ "cat",     "amsterdam",    "drops" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.join(d2, "column_a", "column_d");

    d1.sort("column_a", "column_f").getColumnsAndRecords(function (columns, rows) {
        deepEqual(columns, {
            column_a: {
                aggType   : "max",
                sortType  : "alpha",
                title     : "column_a",
                name      : "column_a",
                isVisible : true,
                index     : 0
            },
            column_b: {
                aggType   : "max",
                sortType  : "alpha",
                title     : "column_b",
                name      : "column_b",
                isVisible : true,
                index     : 1
            },
            column_c: {
                aggType   : "max",
                sortType  : "alpha",
                title     : "column_c",
                name      : "column_c",
                isVisible : true,
                index     : 2
            },
            column_d: {
                aggType   : "max",
                sortType  : "alpha",
                title     : "column_d",
                name      : "column_d",
                isVisible : true,
                index     : 3
            },
            column_e: {
                aggType   : "max",
                sortType  : "alpha",
                title     : "column_e",
                name      : "column_e",
                isVisible : true,
                index     : 4
            },
            column_f: {
                aggType   : "max",
                sortType  : "alpha",
                title     : "column_f",
                name      : "column_f",
                isVisible : true,
                index     : 5
            }
        });
        deepEqual(rows, [
            [ "apple", "violin", "music", "apple",    "screen", "phone" ],
            [ "banana", "piano", "gum",  "banana",     "power", "apple" ],
            [ "cat",   "tissue", "dog",     "cat",     "bagel", "chips" ],
            [ "cat",   "tissue", "dog",     "cat", "amsterdam", "drops" ]
        ]);

        start();
    }).finish();
});

asyncTest("join (left outer join on single field)", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "dump",    "amsterdam",    "drops" ]
    ];
    var dataset2 = [
        [ "column_d", "column_e", "column_f" ],

        [ "banana",      "power",    "apple" ],
        [ "apple",      "screen",    "phone" ],
        [ "cat",         "bagel",    "chips" ],
        [ "car",          "nuts",     "axes" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.join(d2, "column_a", "column_d", "left");

    d1.sort("column_a", "column_f").getDataset(function (result) {
        deepEqual(result, [
            [ "apple",   "violin", "music",  "apple",    "screen", "phone" ],
            [ "banana",   "piano",   "gum", "banana",     "power", "apple" ],
            [ "cat",     "tissue",   "dog",    "cat",     "bagel", "chips" ],
            [ "dump", "amsterdam", "drops",       "",          "",      "" ],
        ]);
        start();
    }).finish();
});

asyncTest("join (right outer join on single field", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "dump",    "amsterdam",    "drops" ]
    ];
    var dataset2 = [
        [ "column_d", "column_e", "column_f" ],

        [ "banana",      "power",    "apple" ],
        [ "apple",      "screen",    "phone" ],
        [ "cat",         "bagel",    "chips" ],
        [ "car",          "nuts",     "axes" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.join(d2, "column_a", "column_d", "right");

    d1.sort("column_a", "column_f").getDataset(function (result) {
        deepEqual(result, [
            [ "",              "",      "",    "car",      "nuts",  "axes" ],
            [ "apple",   "violin", "music",  "apple",    "screen", "phone" ],
            [ "banana",   "piano",   "gum", "banana",     "power", "apple" ],
            [ "cat",     "tissue",   "dog",    "cat",     "bagel", "chips" ]
        ]);
        start();
    }).finish();
});

asyncTest("join (inner join on multiple fields)", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_d", "column_e", "column_f" ],

        [ "banana",      "power",    "apple" ],
        [ "apple",      "screen",    "phone" ],
        [ "cat",         "bagel",    "chips" ],
        [ "cat",     "amsterdam",    "drops" ],
        [ "cat",        "tissue",    "drops" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.join(
        d2,
        [ "column_a", "column_b" ],
        [ "column_d", "column_e" ]
    );

    d1.getDataset(function (result) {
        deepEqual(result, [
            [ "cat", "tissue", "dog", "cat", "tissue", "drops" ]
        ]);
        start();
    }).finish();
});

asyncTest("failed join (unknown join type)", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_d", "column_e", "column_f" ],

        [ "banana",      "power",    "apple" ],
        [ "apple",      "screen",    "phone" ],
        [ "cat",         "bagel",    "chips" ],
        [ "cat",     "amsterdam",    "drops" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.onError(function (error) {
        equal(error, "Unknown join type.");

        start();
    });

    d1.join(d2, "column_a", "column_d", "crazy").finish();
});

asyncTest("failed join (columns with same name)", function () {
    expect(1);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_d", "column_e", "column_c" ],

        [ "banana",      "power",    "apple" ],
        [ "apple",      "screen",    "phone" ],
        [ "cat",         "bagel",    "chips" ],
        [ "cat",     "amsterdam",    "drops" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.onError(function (error) {
        equal(error, "Column names overlap.");

        start();
    });

    d1.join(d2, "column_a", "column_d").finish();
});

asyncTest("prepend column names", function () {
    expect(1);

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "max" },
            { name: "column_c", sortType: "alpha", aggType: "min" }
        ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).prependColumnNames("p_").getColumns(function (columns) {
        deepEqual(columns, {
            p_column_a: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_a",
                name      : "column_a",
                isVisible : true,
                index     : 0
            },
            p_column_b: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_b",
                name      : "column_b",
                isVisible : true,
                index     : 1
            },
            p_column_c: {
                sortType  : "alpha",
                aggType   : "min",
                title     : "column_c",
                name      : "column_c",
                isVisible : true,
                index     : 2
            }
        });

        start();
    }).finish();
});

asyncTest("alter column name", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnName("column_a", "a_column");

    d.getColumns(function (columns) {
        deepEqual(columns, {
            "a_column": {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_a",
                name      : "column_a",
                isVisible : true,
                index     : 0
            },
            "column_b": {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_b",
                name      : "column_b",
                isVisible : true,
                index     : 1
            },
            "column_c": {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_c",
                name      : "column_c",
                isVisible : true,
                index     : 2
            }
        });
        start();
    }).finish();
});

asyncTest("alter column name (fails if changing to already existing column name)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).onError(function (error) {
        equal(error, "Column column_b already exists in the dataset.");

        start();
    });

    d.alterColumnName("column_a", "column_b").finish();
});

asyncTest("alter column sort type", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnSortType("column_a", "random");

    d.getColumns(function (columns) {
        equal(columns["column_a"]["sortType"], "random");
        start();
    }).finish();
});

asyncTest("alter column aggregate type", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnAggregateType("column_a", "random");

    d.getColumns(function (columns) {
        equal(columns["column_a"]["aggType"], "random");
        start();
    }).finish();
});

asyncTest("alter column title", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnTitle("column_a", "random");

    d.getColumns(function (columns) {
        equal(columns["column_a"]["title"], "random");
        start();
    }).finish();
});

asyncTest("group (single field sum)", function () {
    expect(1);

    var dataset = [
        [
            { name : "column_a", sortType : "alpha", aggType : "max" },
            { name : "column_b", sortType : "num",   aggType : "sum" },
        ],

        [ "apple",  453 ],
        [ "cat",    663 ],
        [ "apple",  123 ],
        [ "gummy",  34  ]
    ];

    var d = new DataWorker(dataset).group("column_a").sort("column_a");

    d.getDataset(function (result) {
        deepEqual(result, [
            [ "apple", 576 ],
            [ "cat",   663 ],
            [ "gummy",  34 ]
        ]);
        start();
    }).finish();
});

asyncTest("group (single field max)", function () {
    expect(1);

    var dataset = [
        [
            { name : "column_a", sortType : "alpha", aggType : "max" },
            { name : "column_b", sortType : "num",   aggType : "max" },
        ],

        [ "apple",  453 ],
        [ "cat",    663 ],
        [ "apple",  123 ],
        [ "gummy",  34  ]
    ];

    var d = new DataWorker(dataset).group("column_a").sort("column_a");

    d.getDataset(function (result) {
        deepEqual(result, [
            [ "apple", 453 ],
            [ "cat",   663 ],
            [ "gummy",  34 ]
        ]);
        start();
    }).finish();
});

asyncTest("group (single field min)", function () {
    expect(1);

    var dataset = [
        [
            { name : "column_a", sortType : "alpha", aggType : "max" },
            { name : "column_b", sortType : "num",   aggType : "min" },
        ],

        [ "apple",  453 ],
        [ "cat",    663 ],
        [ "apple",  123 ],
        [ "gummy",  34  ]
    ];

    var d = new DataWorker(dataset).group("column_a").sort("column_a");

    d.getDataset(function (result) {
        deepEqual(result, [
            [ "apple", 123 ],
            [ "cat",   663 ],
            [ "gummy",  34 ]
        ]);
        start();
    }).finish();
});

asyncTest("group (multi-field)", function () {
    expect(1);

    var dataset = [
        [
            { name : "column_a", sortType : "alpha", aggType : "max" },
            { name : "column_b", sortType : "alpha", aggType : "max" },
            { name : "column_c", sortType : "num",   aggType : "sum" },
        ],

        [ "apple",  "violin", 453 ],
        [ "cat",    "tissue", 663 ],
        [ "apple",  "piano",  123 ],
        [ "gummy",  "power",  34  ],
        [ "apple",  "piano",  95  ],
        [ "gummy",  "power",  768 ]
    ];

    var d = new DataWorker(dataset).group("column_a", "column_b").sort("column_a", "column_b");

    d.getDataset(function (result) {
        deepEqual(result, [
            [ "apple",  "piano",  218 ],
            [ "apple",  "violin", 453 ],
            [ "cat",    "tissue", 663 ],
            [ "gummy",   "power", 802 ]
        ]);
        start();
    }).finish();
});

asyncTest("apply filter operates on partitioned datasets as well", function () {
    expect(4);

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" }
        ],

        [ "banana",      "piano",      "gum" ],
        [ "apple",      "violin",    "music" ],
        [ "cat",       "nothing",      "dog" ],
        [ "banana",   "eyedrops",      "tie" ],
        [ "apple",         "gum",   "wallet" ],
        [ "apple",         "gum",     "trix" ],
        [ "gum",           "gun",     "trix" ]
    ];

    var d = new DataWorker(dataset).partition("column_a").applyFilter(/gum/);

    var applePartition, bananaPartition, catPartition, gumPartition;

    d.render(function () {
        deepEqual(
            applePartition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ "apple",         "gum",     "trix" ],
                [ "apple",         "gum",   "wallet" ]
            ]
        );
        deepEqual(
            bananaPartition,
            [
                [ "banana",      "piano",      "gum" ],
            ]
        );
        deepEqual(
            catPartition,
            [ ]
        );
        deepEqual(
            gumPartition,
            [
                [ "gum",           "gun",     "trix" ]
            ]
        );

        start();
    });

    d.getPartitioned(function (partition) {
        applePartition = partition;
    }, "apple");
    d.getPartitioned(function (partition) {
        bananaPartition = partition;
    }, "banana");
    d.getPartitioned(function (partition) {
        catPartition = partition;
    }, "cat");
    d.getPartitioned(function (partition) {
        gumPartition = partition;
    }, "gum");

    (function wait() {
        if (
            typeof(applePartition) === "object"
            && typeof(bananaPartition) === "object"
            && typeof(catPartition) === "object"
            && typeof(gumPartition) === "object"
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    })();
});

asyncTest("clear filter operates on partitioned datasets as well", function () {
    expect(4);

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" }
        ],

        [ "banana",      "piano",      "gum" ],
        [ "apple",      "violin",    "music" ],
        [ "cat",       "nothing",      "dog" ],
        [ "banana",   "eyedrops",      "tie" ],
        [ "apple",         "gum",   "wallet" ],
        [ "apple",         "gum",     "trix" ],
        [ "gum",           "gun",     "trix" ]
    ];

    var d = new DataWorker(dataset).partition("column_a").applyFilter(/gum/).clearFilters();

    var applePartition, bananaPartition, catPartition, gumPartition;

    d.render(function () {
        deepEqual(
            applePartition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ "apple",      "violin",    "music" ],
                [ "apple",         "gum",     "trix" ],
                [ "apple",         "gum",   "wallet" ]
            ]
        );
        deepEqual(
            bananaPartition,
            [
                [ "banana",      "piano",      "gum" ],
                [ "banana",   "eyedrops",      "tie" ]
            ]
        );
        deepEqual(
            catPartition,
            [
                [ "cat",       "nothing",      "dog" ]
            ]
        );
        deepEqual(
            gumPartition,
            [
                [ "gum",           "gun",     "trix" ]
            ]
        );

        start();
    });

    d.getPartitioned(function (partition) {
        applePartition = partition;
    }, "apple");
    d.getPartitioned(function (partition) {
        bananaPartition = partition;
    }, "banana");
    d.getPartitioned(function (partition) {
        catPartition = partition;
    }, "cat");
    d.getPartitioned(function (partition) {
        gumPartition = partition;
    }, "gum");

    (function wait() {
        if (
            typeof(applePartition) === "object"
            && typeof(bananaPartition) === "object"
            && typeof(catPartition) === "object"
            && typeof(gumPartition) === "object"
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    })();
});

asyncTest("partitioned datasets obeys hidden columns", function () {
    expect(4);

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" }
        ],

        [ "banana",      "piano",      "gum" ],
        [ "apple",      "violin",    "music" ],
        [ "cat",       "nothing",      "dog" ],
        [ "banana",   "eyedrops",      "tie" ],
        [ "apple",         "gum",   "wallet" ],
        [ "apple",         "gum",     "trix" ],
        [ "gum",           "gun",     "trix" ]
    ];

    var d = new DataWorker(dataset).partition("column_a").hideColumns("column_a");

    var applePartition, bananaPartition, catPartition, gumPartition;

    d.render(function () {
        deepEqual(
            applePartition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ "violin",    "music" ],
                [    "gum",     "trix" ],
                [    "gum",   "wallet" ]
            ]
        );
        deepEqual(
            bananaPartition,
            [
                [    "piano",      "gum" ],
                [ "eyedrops",      "tie" ]
            ]
        );
        deepEqual(
            catPartition,
            [
                [ "nothing",      "dog" ]
            ]
        );
        deepEqual(
            gumPartition,
            [
                [ "gun",     "trix" ]
            ]
        );

        start();
    });

    d.getPartitioned(function (partition) {
        applePartition = partition;
    }, "apple");
    d.getPartitioned(function (partition) {
        bananaPartition = partition;
    }, "banana");
    d.getPartitioned(function (partition) {
        catPartition = partition;
    }, "cat");
    d.getPartitioned(function (partition) {
        gumPartition = partition;
    }, "gum");

    (function wait() {
        if (
            typeof(applePartition) === "object"
            && typeof(bananaPartition) === "object"
            && typeof(catPartition) === "object"
            && typeof(gumPartition) === "object"
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    })();
});

asyncTest("get partitioned (single field)", function () {
    expect(5);

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" }
        ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ],
        [ "apple",      "trance",   "camaro" ],
        [ "cat",           "soy",  "blender" ],
        [ "banana",   "eyedrops",      "tie" ],
        [ "apple",        "body",      "key" ]
    ];

    var d = new DataWorker(dataset).partition("column_a");

    var partitionKeys,
        applePartition  = [],
        bananaPartition = [],
        catPartition    = [],
        gummyPartition  = [];

    d.render(function () {
        deepEqual(partitionKeys.sort(), [
            [ "apple"  ],
            [ "banana" ],
            [ "cat"    ],
            [ "gummy"  ]
        ]);

        deepEqual(
            applePartition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ "apple",   "body",    "key" ],
                [ "apple", "trance", "camaro" ],
                [ "apple", "violin",  "music" ]
            ]
        );

        deepEqual(
            bananaPartition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ "banana", "eyedrops", "tie" ],
                [ "banana",    "piano", "gum" ]
            ]
        );

        deepEqual(
            catPartition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ "cat",    "soy", "blender" ],
                [ "cat", "tissue",     "dog" ]
            ]
        );

        deepEqual(gummyPartition, [
            [ "gummy", "power", "star" ]
        ]);

        start();
    });

    d.getPartitionKeys(function (keys) {
        partitionKeys = keys;
    });

    d.getPartitioned(function (partition) {
        applePartition = partition;
    }, "apple");

    d.getPartitioned(function (partition) {
        bananaPartition = partition;
    }, "banana");

    d.getPartitioned(function (partition) {
        catPartition = partition;
    }, "cat");

    d.getPartitioned(function (partition) {
        gummyPartition = partition;
    }, "gummy");

    var wait = function () {
        if (
            applePartition.length > 0
            && bananaPartition.length > 0
            && catPartition.length > 0
            && gummyPartition.length > 0
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
});

asyncTest("get partitioned (multi-field)", function () {
    expect(7);

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" },
        ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ],
        [ "apple",      "trance",   "camaro" ],
        [ "cat",           "soy",  "blender" ],
        [ "banana",      "piano",      "tie" ],
        [ "apple",      "violin",      "key" ]
    ];

    var d = new DataWorker(dataset).partition("column_a", "column_b");

    var partitionKeys,
        appleTrancePartition = [],
        appleViolinPartition = [],
        bananaPianoPartition = [],
        catSoyPartition      = [],
        catTissuePartition   = [],
        gummyPowerPartition  = [];

    d.render(function () {
        deepEqual(partitionKeys.sort(), [
            [ "apple", "trance" ],
            [ "apple", "violin" ],
            [ "banana", "piano" ],
            [ "cat",      "soy" ],
            [ "cat",   "tissue" ],
            [ "gummy", "power"  ]
        ]);

        deepEqual(appleTrancePartition, [
            [ "apple", "trance", "camaro" ]
        ]);

        deepEqual(
            appleViolinPartition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ "apple", "violin",   "key" ],
                [ "apple", "violin", "music" ]
            ]
        );

        deepEqual(
            bananaPianoPartition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ "banana", "piano", "gum" ],
                [ "banana", "piano", "tie" ]
            ]
        );

        deepEqual(catSoyPartition, [
            [ "cat", "soy",  "blender" ]
        ]);

        deepEqual(catTissuePartition, [
            [ "cat", "tissue", "dog" ]
        ]);

        deepEqual(gummyPowerPartition, [
            [ "gummy", "power", "star" ]
        ]);

        start();
    });

    d.getPartitionKeys(function (keys) {
        partitionKeys = keys;
    });

    d.getPartitioned(function (partition) {
        appleTrancePartition = partition;
    }, "apple", "trance");

    d.getPartitioned(function (partition) {
        appleViolinPartition = partition;
    }, "apple", "violin");

    d.getPartitioned(function (partition) {
        bananaPianoPartition = partition;
    }, "banana", "piano");

    d.getPartitioned(function (partition) {
        catSoyPartition = partition;
    }, "cat", "soy");

    d.getPartitioned(function (partition) {
        catTissuePartition = partition;
    }, "cat", "tissue");

    d.getPartitioned(function (partition) {
        gummyPowerPartition = partition;
    }, "gummy", "power");

    var wait = function () {
        if (
            appleTrancePartition.length > 0
            && appleViolinPartition.length > 0
            && bananaPianoPartition.length > 0
            && catSoyPartition.length > 0
            && catTissuePartition.length > 0
            && gummyPowerPartition.length > 0
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
});

asyncTest("clone", function () {
    expect(3);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.clone(function (clone) {
        ok(clone instanceof DataWorker);

        clone.getColumnsAndRecords(function (columns, records) {
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
    });
});

asyncTest("get rows (all)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);

        start();
    });
});

asyncTest("get rows (specify start)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);

        start();
    }, 2);
});

asyncTest("get rows (specify end)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);

        start();
    }, undefined, 1);
});

asyncTest("get rows (specify start and end)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ]
        ]);

        start();
    }, 1, 2);
});

asyncTest("get rows (specify a too-large end)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);

        start();
    }, 1, 10);
});

asyncTest("get rows (specify columns)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "cat",     "dog" ],
            [ "banana",  "gum" ],
            [ "gummy",  "star" ]
        ]);

        start();
    }, 1, 10, "column_a", "column_c");
});

asyncTest("get rows (specify columns as array)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "cat",     "dog" ],
            [ "banana",  "gum" ],
            [ "gummy",  "star" ]
        ]);

        start();
    }, 1, 10, [ "column_a", "column_c" ]);
});

asyncTest("get rows (specify out-of-order columns)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        deepEqual(result, [
            [ "dog", "cat" ]
        ]);

        start();
    }, 1, 1, "column_c", "column_a");
});

asyncTest("get number of records", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getNumberOfRecords(function (result) {
        equal(result, 4);
        start();
    });
});

asyncTest("getRows obeys applied filter", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/apple/);

    d.getRows(function(result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("getColumnsAndRecords obeys applied filter", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/apple/);

    d.getColumnsAndRecords(function(columns, rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("setDecimalMarkCharacter", function () {
    expect(2);

    var dataset = [
        [ "column_a" ],

        [ "1.435"    ],
        [ "3,600"    ],
        [ "4.560"     ],
        [ "2,345"    ]
    ];

    var d = new DataWorker(dataset);

    d.alterColumnSortType("column_a", "num")
     .sort("column_a")
     .getColumnsAndRecords(function (columns, rows) {
        deepEqual(rows, [
            [ "1.435" ],
            [ "4.560" ],
            [ "2,345" ],
            [ "3,600" ]
        ]);
     }).setDecimalMarkCharacter(",")
     .sort("column_a")
     .getColumnsAndRecords(function (columns, rows) {
        deepEqual(rows, [
            [ "2,345" ],
            [ "3,600" ],
            [ "1.435" ],
            [ "4.560" ]
        ]);
        start();
     }).finish();
});

asyncTest("getDistinctConsecutiveRows", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b" ],

        [ "abc",      "123"      ],
        [ "abc",      "456"      ],
        [ "abc",      "789"      ],
        [ "def",      "123"      ],
        [ "ghi",      "123"      ],
        [ "ghi",      "456"      ],
        [ "def",      "456"      ],
        [ "def",      "789"      ]
    ];

    var d = new DataWorker(dataset);

    d.alterColumnSortType("column_a", "num")
     .sort("column_a")
     .getDistinctConsecutiveRows(function (rows) {
        deepEqual(rows, [
            [ "abc", 0, 2 ],
            [ "def", 3, 3 ],
            [ "ghi", 4, 5 ],
            [ "def", 6, 7 ]
        ]);
     }, "column_a").getDistinctConsecutiveRows(function (rows) {
        deepEqual(rows, [
            [ "123", 0, 0 ],
            [ "456", 1, 1 ],
            [ "789", 2, 2 ],
            [ "123", 3, 4 ],
            [ "456", 5, 6 ],
            [ "789", 7, 7 ]
        ]);
        start();
     }, "column_b").finish();
});

asyncTest("extraColumnInfoGetsPassedAlong", function () {
    expect(2);

    var dataset = [
        [
            {
                name        : "column_a",
                abc         : "xyz",
                randomStuff : "still here"
            },
            {
                name      : "column_b",
                elephants : "donkeys"
            }
        ],

        [ "apple",      "violin" ],
        [ "cat",        "tissue" ],
        [ "banana",      "piano" ],
        [ "gummy",       "power" ]
    ];

    var d = new DataWorker(dataset);

    ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
        deepEqual(columns, {
            column_a: {
                sortType    : "alpha",
                aggType     : "max",
                title       : "column_a",
                name        : "column_a",
                isVisible   : true,
                index       : 0,
                abc         : "xyz",
                randomStuff : "still here"
            },
            column_b: {
                sortType    : "alpha",
                aggType     : "max",
                title       : "column_b",
                name        : "column_b",
                isVisible   : true,
                index       : 1,
                elephants   : "donkeys"
            }
        });

        start();
    }).finish();
});

asyncTest("hide columns (single)", function () {
    expect(4);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.hideColumns("column_a").getColumnsAndRecords(function (columns, records) {
        deepEqual(columns, {
            column_b: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_b",
                name      : "column_b",
                isVisible : true,
                index     : 0
            },
            column_c: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_c",
                name      : "column_c",
                isVisible : true,
                index     : 1
            }
        });

        deepEqual(records, [
            [ "violin",    "music" ],
            [ "tissue",      "dog" ],
            [  "piano",      "gum" ],
            [  "power",     "star" ]
        ]);
    }).getAllColumnsAndAllRecords(function (columns, records) {
        deepEqual(columns, {
            column_a: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_a",
                name      : "column_a",
                isVisible : false,
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
    }).finish();
});

asyncTest("hide columns (multi)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns("column_a", "column_c");

    d.getColumnsAndRecords(function (columns, records) {
        deepEqual(columns, {
            column_b: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_b",
                name      : "column_b",
                isVisible : true,
                index     : 0
            }
        });

        deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);

        start();
    }).finish();
});

asyncTest("hide column that does not exist does not error out", function () {
    expect(0);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).render(function () { start(); })
                              .hideColumns("column_d")
                              .render()
                              .finish();
});

asyncTest("hide columns (regex)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "notcolumn_b", "column_c" ],

        [ "apple",         "violin",    "music" ],
        [ "cat",           "tissue",      "dog" ],
        [ "banana",         "piano",      "gum" ],
        [ "gummy",          "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns(/^column/);

    d.getColumnsAndRecords(function (columns, records) {
        deepEqual(columns, {
            notcolumn_b : {
                sortType  : "alpha",
                aggType   : "max",
                title     : "notcolumn_b",
                name      : "notcolumn_b",
                isVisible : true,
                index     : 0
            }
        });

        deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);

        start();
    }).finish();
});

asyncTest("hide columns (regex, w/ flags)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "notcolumn_b", "COLumn_c" ],

        [ "apple",         "violin",    "music" ],
        [ "cat",           "tissue",      "dog" ],
        [ "banana",         "piano",      "gum" ],
        [ "gummy",          "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns(/^column/i);

    d.getColumnsAndRecords(function (columns, records) {
        deepEqual(columns, {
            notcolumn_b : {
                sortType  : "alpha",
                aggType   : "max",
                title     : "notcolumn_b",
                name      : "notcolumn_b",
                isVisible : true,
                index     : 0
            }
        });

        deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);

        start();
    }).finish();
});

asyncTest("getColumns respects hidden columns", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.hideColumns("column_b").getColumns(function (columns) {
        deepEqual(Object.keys(columns), [ "column_a", "column_c" ]);
        start();
    }).finish();
});

asyncTest("show columns", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns("column_a", "column_c")
                              .showColumns("column_a");

    d.getColumnsAndRecords(function (columns, records) {
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
            }
        });

        deepEqual(records, [
            [ "apple",      "violin" ],
            [ "cat",        "tissue" ],
            [ "banana",      "piano" ],
            [ "gummy",       "power" ]
        ]);

        start();
    }).finish();
});

asyncTest("show column that does not exist does not error out", function () {
    expect(0);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).render(function () { start(); })
                              .showColumns("column_d")
                              .render()
                              .finish();
});

asyncTest("show columns (regex)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "notcolumn_b", "COLumn_c" ],

        [ "apple",         "violin",    "music" ],
        [ "cat",           "tissue",      "dog" ],
        [ "banana",         "piano",      "gum" ],
        [ "gummy",          "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns("column_a", "notcolumn_b", "COLumn_c")
                              .showColumns(/^(?:not)?column_/);

    d.getColumnsAndRecords(function (columns, records) {
        deepEqual(columns, {
            column_a: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_a",
                name      : "column_a",
                isVisible : true,
                index     : 0
            },
            notcolumn_b: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "notcolumn_b",
                name      : "notcolumn_b",
                isVisible : true,
                index     : 1
            }
        });

        deepEqual(records, [
            [ "apple",      "violin" ],
            [ "cat",        "tissue" ],
            [ "banana",      "piano" ],
            [ "gummy",       "power" ]
        ]);

        start();
    }).finish();
});

asyncTest("hide all columns" , function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns("column_a", "column_c")
                              .hideAllColumns();

    d.getColumnsAndRecords(function (columns, records) {
        deepEqual(columns, {});
        deepEqual(records, [ [], [], [], [] ]);

        start();
    }).finish();
});

asyncTest("show all columns", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns("column_a", "column_c")
                              .showAllColumns();

    d.getColumnsAndRecords(function (columns, records) {
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
    }).finish();
});

asyncTest("changes for \"on_\" functions are added to the queue by default", function () {
    expect(3);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d._actionQueue._isInAction = true;

    d.onError(function (error) {
        equal(error, "Column column_b already exists in the dataset.");

        d.finish();
        start();
    });

    equal(d._actionQueue._queueStack.length, 1);
    equal(d._actionQueue._queueStack[0].length, 1);

    d.alterColumnName("column_a", "column_b");

    d._finishAction();
});

asyncTest("changes for \"on_\" functions can happen immediately with a flag", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d._actionQueue._isInAction = true;

    d.onError(function (error) {
        equal(error, "Column column_b already exists in the dataset.");

        d.finish();
        start();
    }, true);

    equal(d._actionQueue._queueStack.length, 0);

    d.alterColumnName("column_a", "column_b");

    d._finishAction();
});

asyncTest("add child rows", function () {
    expect(2);

    var parentDataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ], childRows = [
        [ "apple",    "fuji",          "red"      ],
        [ "apple",    "red delicious", "red"      ],
        [ "apple",    "granny smith",  "green"    ],
        [ "apple",    "honey crisp",   "yellow"   ],

        [ "cat",      "siamese",       "tall"     ],
        [ "cat",      "sphynx",        "bald"     ],
        [ "cat",      "calico",        "rainbow"  ],

        [ "gummy",    "yummy",         "funny"    ]
    ];

    var d = new DataWorker(parentDataset);

    d.addChildRows(childRows, "column_a");

    d.getNumberOfRecords(function (num) {
        equal(num, 12);
    });

    d.getDataset(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin",        "music"   ],
                [ "apple",  "fuji",          "red"     ],
                [ "apple",  "red delicious", "red"     ],
                [ "apple",  "granny smith",  "green"   ],
                [ "apple",  "honey crisp",   "yellow"  ],
            [ "cat",    "tissue",        "dog"     ],
                [ "cat",    "siamese",       "tall"    ],
                [ "cat",    "sphynx",        "bald"    ],
                [ "cat",    "calico",        "rainbow" ],
            [ "banana", "piano",         "gum"     ],
            [ "gummy",  "power",         "star"    ],
                [ "gummy",  "yummy",         "funny"   ]
        ]);

        start();
    }).finish();
});

asyncTest("add child rows using another DataWorker object", function () {
    expect(2);

    var parentDataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ], childDataset = [
        [ "column_a", "column_b",      "column_c" ],

        [ "apple",    "fuji",          "red"      ],
        [ "apple",    "red delicious", "red"      ],
        [ "apple",    "granny smith",  "green"    ],
        [ "apple",    "honey crisp",   "yellow"   ],

        [ "cat",      "siamese",       "tall"     ],
        [ "cat",      "sphynx",        "bald"     ],
        [ "cat",      "calico",        "rainbow"  ],

        [ "gummy",    "yummy",         "funny"    ]
    ];

    var d = new DataWorker(parentDataset);
    var d2 = new DataWorker(childDataset);

    d.addChildRows(d2, "column_a");

    d.getNumberOfRecords(function (num) {
        equal(num, 12);
    });

    d.getDataset(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin",        "music"   ],
                [ "apple",  "fuji",          "red"     ],
                [ "apple",  "red delicious", "red"     ],
                [ "apple",  "granny smith",  "green"   ],
                [ "apple",  "honey crisp",   "yellow"  ],
            [ "cat",    "tissue",        "dog"     ],
                [ "cat",    "siamese",       "tall"    ],
                [ "cat",    "sphynx",        "bald"    ],
                [ "cat",    "calico",        "rainbow" ],
            [ "banana", "piano",         "gum"     ],
            [ "gummy",  "power",         "star"    ],
                [ "gummy",  "yummy",         "funny"   ]
        ]);

        start();
    }).finish();
});

asyncTest("children of invisible parents default to invisible", function () {
    expect(2);

    var parentDataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ], childRows = [
        [ "apple",    "fuji",          "red"      ],
        [ "apple",    "red delicious", "red"      ],
        [ "apple",    "granny smith",  "green"    ],
        [ "apple",    "honey crisp",   "yellow"   ],

        [ "cat",      "siamese",       "tall"     ],
        [ "cat",      "sphynx",        "bald"     ],
        [ "cat",      "calico",        "rainbow"  ],

        [ "gummy",    "yummy",         "funny"    ]
    ];

    var d = new DataWorker(parentDataset);

    d.applyFilter(/apple/)
     .addChildRows(childRows, "column_a");

    d.getNumberOfRecords(function (num) {
        equal(num, 5);
    });

    d.getDataset(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin",        "music"   ],
                [ "apple",  "fuji",          "red"     ],
                [ "apple",  "red delicious", "red"     ],
                [ "apple",  "granny smith",  "green"   ],
                [ "apple",  "honey crisp",   "yellow"  ]
        ]);

        start();
    }).finish();
});

asyncTest("sorts children as subsets of parents, not as part of the whole dataset", function () {
    expect(2);

    var parentDataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ], childRows = [
        [ "apple",    "fuji",          "red"      ],
        [ "apple",    "red delicious", "red"      ],
        [ "apple",    "granny smith",  "green"    ],
        [ "apple",    "honey crisp",   "yellow"   ],

        [ "cat",      "siamese",       "tall"     ],
        [ "cat",      "sphynx",        "bald"     ],
        [ "cat",      "calico",        "rainbow"  ],

        [ "gummy",    "yummy",         "funny"    ]
    ];

    var d = new DataWorker(parentDataset);

    d.addChildRows(childRows, "column_a");

    d.getNumberOfRecords(function (num) {
        equal(num, 12);
    });

    d.sort("column_b").getDataset(function (rows) {
        deepEqual(rows, [
            [ "banana", "piano",         "gum"     ],
            [ "gummy",  "power",         "star"    ],
                [ "gummy",  "yummy",         "funny"   ],
            [ "cat",    "tissue",        "dog"     ],
                [ "cat",    "calico",        "rainbow" ],
                [ "cat",    "siamese",       "tall"    ],
                [ "cat",    "sphynx",        "bald"    ],
            [ "apple",  "violin",        "music"   ],
                [ "apple",  "fuji",          "red"     ],
                [ "apple",  "granny smith",  "green"   ],
                [ "apple",  "honey crisp",   "yellow"  ],
                [ "apple",  "red delicious", "red"     ]
        ]);

        start();
    }).finish();
});

asyncTest("sorts children within parents when parents have ties", function () {
    expect(2);

    var parentDataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "parent",   "music"    ],
        [ "cat",      "parent",   "dog"      ],
        [ "banana",   "parent",   "gum"      ],
        [ "gummy",    "parent",   "star"     ]
    ], childRows = [
        [ "apple",    "fuji",          "red"      ],
        [ "apple",    "red delicious", "red"      ],
        [ "apple",    "granny smith",  "green"    ],
        [ "apple",    "honey crisp",   "yellow"   ],

        [ "cat",      "siamese",       "tall"     ],
        [ "cat",      "sphynx",        "bald"     ],
        [ "cat",      "calico",        "rainbow"  ],

        [ "gummy",    "yummy",         "funny"    ]
    ];

    var d = new DataWorker(parentDataset);

    d.addChildRows(childRows, "column_a");

    d.getNumberOfRecords(function (num) {
        equal(num, 12);
    });

    d.sort("column_b").getDataset(function (rows) {
        deepEqual(rows, [
            [ "apple",  "parent",        "music"   ],
                [ "apple",  "fuji",          "red"     ],
                [ "apple",  "granny smith",  "green"   ],
                [ "apple",  "honey crisp",   "yellow"  ],
                [ "apple",  "red delicious", "red"     ],
            [ "cat",    "parent",        "dog"     ],
                [ "cat",    "calico",        "rainbow" ],
                [ "cat",    "siamese",       "tall"    ],
                [ "cat",    "sphynx",        "bald"    ],
            [ "banana", "parent",        "gum"     ],
            [ "gummy",  "parent",        "star"    ],
                [ "gummy",  "yummy",         "funny"   ]
        ]);

        start();
    }).finish();
});

asyncTest("multiple column sort with nulls in number columns", function () {
    expect(1);

    var d = new DataWorker([
        [ "numeric_column", "alpha_column" ],

        [ null,             "xyz"          ],
        [ null,             "abc"          ],
        [ null,             "lmnop"        ],
        [ null,             "def"          ]
    ]);

    d.alterColumnSortType("numeric_column", "num");

    d.sort("numeric_column", "alpha_column").getDataset(function (rows) {
        deepEqual(rows, [
            [ null, "abc"   ],
            [ null, "def"   ],
            [ null, "lmnop" ],
            [ null, "xyz"   ]
        ]);

        start();
    }).finish();
});

asyncTest("getDistinctConsecutiveRows with child rows just looks at parent data", function () {
    expect(5);

    var dataset = [
        [ "rank", "name",  "side", "amount" ],

        [ 1,      "One",   null,        500 ],
        [ 2,      "Two",   null,        500 ],
        [ 2,      "Three", null,        100 ],
        [ 4,      "Four",  null,        300 ]
    ], childRows = [
        [ null, "One",   "Left",   400 ],
        [ null, "One",   "Right",   50 ],
        [ null, "One",   "Top",     50 ],

        [ null, "Two",   "Top",    500 ],

        [ null, "Three", "Left",    25 ],
        [ null, "Three", "Bottom",  75 ],

        [ null, "Four",  "Middle", 150 ],
        [ null, "Four",  "Top",    100 ],
        [ null, "Four",  "Left",    25 ],
        [ null, "Four",  "Right",   25 ]
    ];

    var d = new DataWorker(dataset);
    d.addChildRows(childRows, "name");

    d.getNumberOfRecords(function (num) {
        equal(num, 14);
    });

    d.alterColumnSortType("rank", "num")
     .alterColumnSortType("amount", "num")
     .sort("-amount", "rank", "side")
     .getDataset(function (rows) {
        deepEqual(rows, [
            [ 1,    "One",   null,     500 ],
                [ null, "One",   "Left",   400 ],
                [ null, "One",   "Right",   50 ],
                [ null, "One",   "Top",     50 ],
            [ 2,    "Two",   null,     500 ],
                [ null, "Two",   "Top",    500 ],
            [ 4,    "Four",  null,     300 ],
                [ null, "Four",  "Middle", 150 ],
                [ null, "Four",  "Top",    100 ],
                [ null, "Four",  "Left",    25 ],
                [ null, "Four",  "Right",   25 ],
            [ 2,    "Three", null,     100 ],
                [ null, "Three", "Bottom",  75 ],
                [ null, "Three", "Left",    25 ],
        ]);
     }).getDistinctConsecutiveRows(function (rows) {
        deepEqual(rows, [
            [ "One",    0,  3 ],
            [ "Two",    4,  5 ],
            [ "Four",   6, 10 ],
            [ "Three", 11, 13 ]
        ]);
     }, "name").getDistinctConsecutiveRows(function (rows) {
        deepEqual(rows, [
            [ null, 0, 13 ]
        ]);
     }, "side").getDistinctConsecutiveRows(function (rows) {
        deepEqual(rows, [
            [ 500,  0,  5 ],
            [ 300,  6, 10 ],
            [ 100, 11, 13 ]
        ]);
        start();
     }, "amount").finish();
});

asyncTest("DataWorker works without Web Worker support (older browsers)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    Worker = undefined;
    var d = new DataWorker(dataset);
    delete Worker;

    d.applyFilter(/apple/, "column_a").getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
        start();
    }).finish();
});

asyncTest("then() function lets you utilize DataWorker\"s action queue", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var x = 10;

    var d = new DataWorker(dataset);

    d.getNumberOfRecords(function (numRows) {
        equal(x, 10);
        x = numRows;
    }).then(function () {
        equal(x, 4);
        start();
    }).finish();
});

asyncTest("clearDataset", function () {
    expect(3);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getDataset(function(result) {
        deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "star"  ]
        ]);
    }).clearDataset().getColumns(function (result) {
        deepEqual(result, {});
    }).getDataset(function (result) {
        deepEqual(result, []);
        start();
    }).finish();
});

asyncTest("appending to empty dataset takes in new columns", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker([ [] ]).append(dataset);

    d.getDataset(function(result) {
        deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "star"  ]
        ]);
        start();
    }).finish();
});

asyncTest("getHashedRows base case", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getHashedRows(function (result) {
        deepEqual(result, [
            {
                "column_a": "apple",
                "column_b": "violin",
                "column_c": "music"
            },
            {
                "column_a": "cat",
                "column_b": "tissue",
                "column_c": "dog"
            },
            {
                "column_a": "banana",
                "column_b": "piano",
                "column_c": "gum"
            },
            {
                "column_a": "gummy",
                "column_b": "power",
                "column_c": "star"
            }
        ]);
        start();
    }).finish();
});

asyncTest("getPage can ask for specific columns", function () {
    expect(6);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.paginate(3).getPage(function (page, pageNum) {
        deepEqual(page, [
            [ "apple",  "violin" ],
            [ "cat",    "tissue" ],
            [ "banana", "piano"  ]
        ]);
        equal(pageNum, 1);
    }, undefined, "column_a", "column_b").getPage(function (page, pageNum) {
        deepEqual(page, [
            [ "gummy",  "star" ]
        ]);
        equal(pageNum, 2);
    }, 2, [ "column_a", "column_c" ]).getPage(function (page, pageNum) {
        deepEqual(page, [
            [ "violin" ],
            [ "tissue" ],
            [ "piano"  ]
        ]);
        equal(pageNum, 1);
        start();
    }, 1, "column_b").finish();
});

asyncTest("get/set summary rows", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.setSummaryRows(
        [ "things", "stuffs", "foothings" ],
        [ "things", "stuffs", "foostuffs" ]
    );

    d.getSummaryRows(function (rows) {
        deepEqual(rows, [
            [ "things", "stuffs", "foothings" ],
            [ "things", "stuffs", "foostuffs" ]
        ]);

        start();
    }).finish();
});

asyncTest("get/set summary rows (set with an array of summary rows)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.setSummaryRows([
        [ "things", "stuffs", "foothings" ],
        [ "things", "stuffs", "foostuffs" ]
    ]);

    d.getSummaryRows(function (rows) {
        deepEqual(rows, [
            [ "things", "stuffs", "foothings" ],
            [ "things", "stuffs", "foostuffs" ]
        ]);

        start();
    }).finish();
});

asyncTest("get summary rows (specify columns)", function () {
    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.setSummaryRows(
        [ "things", "stuffs", "foothings" ],
        [ "things", "stuffs", "foostuffs" ]
    );

    d.getSummaryRows(function (rows) {
        deepEqual(rows, [
            [ "foothings", "things" ],
            [ "foostuffs", "things" ]
        ]);

        start();
    }, "column_c", "column_a").finish();
});

asyncTest("apply filter (simple, column-restricted, multi-column, found, using array for column names)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/apple/, [ "column_a", "column_c" ])
     .sort("column_a")
     .getDataset(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power", "apple"  ]
        ]);
        start();
     }).finish();
});

asyncTest("search (simple)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);
    d.search(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power", "apple"  ]
        ]);
    }, /APPLE/i).getDataset(function (result) {
        deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "apple" ]
        ]);
        start();
    }).finish();
});

asyncTest("search (simple, only searches visible rows)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/i/).getDataset(function (result) {
        deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ]
        ]);
    }).search(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ]
        ]);
        start();
    }, "apple").finish();
});

asyncTest("search (specify columns)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        deepEqual(result, [
            [ "apple", "violin" ]
        ]);
        start();
    }, /apple/, { columns: [ "column_a", "column_b" ] }).finish();
});

asyncTest("search (specify columns, single column without array)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        deepEqual(result, [
            [ "music" ],
            [ "gum"   ]
        ]);
        start();
    }, /u/, { columns: "column_c" }).finish();
});

asyncTest("search (specify sort order)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        deepEqual(result, [
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "apple",  "violin", "music" ]
        ]);
        start();
    }, /i/, { sortOn: [ "-column_a" ] }).finish();
});

asyncTest("search (specify sort order, single column without array)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        deepEqual(result, [
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "apple",  "violin", "music" ]
        ]);
        start();
    }, /i/, { sortOn: "-column_a" }).finish();
});

asyncTest("search (limit number of results)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue", "dog"   ]
        ]);
        start();
    }, /o/, { limit: 2 }).finish();
});

asyncTest("search (sort on columns that aren't being fetched)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset),
        searchOptions = {
            sortOn: "column_b",
            columns: [ "column_a", "column_c" ]
        };

    d.search(function (result) {
        deepEqual(result, [
            [ "banana", "gum"   ],
            [ "gummy",  "apple" ],
            [ "apple",  "music" ]
        ]);
        start();
    }, /[mu]{2}/, searchOptions).finish();
});

asyncTest("search (search different columns than those being returned)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset),
        searchOptions = {
            searchOn: [ "column_a", "column_c" ],
            columns: "column_b"
        };

    d.search(function (result) {
        deepEqual(result, [
            [ "violin" ],
            [ "piano"  ],
            [ "power"  ]
        ]);
        start();
    }, /(?:apple|gum)/, searchOptions).finish();
});

asyncTest("search (return different columns than those being searched)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset),
        searchOptions = {
            returnColumns: [ "column_a", "column_c" ]
        };

    d.search(function (result) {
        deepEqual(result, [
            [ "apple",  "music" ],
            [ "banana", "gum"   ],
            [ "gummy",  "apple" ]
        ]);
        start();
    }, /p/, searchOptions).finish();
});

asyncTest("two single-threaded dataworkers", function () {
    expect(2);

    var dataset1 = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];
    var dataset2 = [
        [ "column_a", "column_b", "column_c" ],

        [ "gummy",       "power",    "apple" ],
        [ "car",        "screen",    "phone" ],
        [ "sign",        "bagel",    "chips" ]
    ];

    dataset1.forceSingleThread = 1;
    dataset2.forceSingleThread = 1;

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    d1.sort("column_a");
    d2.sort("column_c");

    d1.getRows(function (rows1) {
        deepEqual(rows1, [
            [ "apple",      "violin",    "music" ],
            [ "banana",      "piano",      "gum" ],
            [ "cat",        "tissue",      "dog" ],
        ]);

        d2.getRows(function (rows2) {
            deepEqual(rows2, [
                [ "gummy",       "power",    "apple" ],
                [ "sign",        "bagel",    "chips" ],
                [ "car",        "screen",    "phone" ]
            ]);

            d1.finish();
            d2.finish();
        });

        start();
    });
});

asyncTest("applying filters to non-existant columns does not break dataworker", function () {
    expect(3);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter(/violin/, [ "column_b", "column_x" ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ]
        ]);
    }).clearDataset().applyFilter(/violin/, "column_b").getRows(function (rows) {
        deepEqual(rows, []);
    }).append(dataset).applyFilter(/violin/, "column_b").getRows(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ]
        ]);

        start();
    }, /violin/).finish();
});

asyncTest("searching non-existant columns does not break dataworker", function () {
    expect(4);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        deepEqual(rows, [
            [ "violin" ]
        ]);
    }, /v/, { columns: [ "column_b", "column_x" ] });

    dw.clearDataset().search(function (rows) {
        deepEqual(rows, []);
    }, /v/, { columns: "column_b" });

    dw.search(function (rows) {
        deepEqual(rows, []);
    }, /v/);

    dw.append(dataset).search(function (rows) {
        deepEqual(rows, [
            [ "violin" ]
        ]);

        start();
    }, /v/, { columns: "column_b" }).finish();
});

asyncTest("allow for blank column titles", function () {
    expect(2);

    var dataset = [
        [ { name: "abc", title: "" }, { name: "123" } ],

        [ "Alphabetic", "Numeric" ],
    ];

    var dw = new DataWorker(dataset);

    dw.getColumns(function (columns) {
        equal(columns["abc"]["title"], "");
        equal(columns["123"]["title"], "123");

        start();
    }).finish();
});

asyncTest("apply filter (complex, gt/e, lt/e, eq, ne)", function () {
    expect(3);

    var numbers = { name: "numbers", sortType: "num" };

    var dataset = [
        [ numbers, "letters",      "category"  ],

        [ 1,       "apple",        "vegetable" ],
        [ "2",     "banana",       "vegetable" ],
        [ 3,       "cat",          "animal"    ],
        [ "10",    "dog",          "animal"    ],
        [ "20",    "calendar",     "mineral"   ],
        [ "30",    "ananas",       "vegetable" ],
        [ 100,     "bandersnatch", "animal"    ],
        [ "200",   "gorilla",      "animal"    ],
        [ 300,     "rock",         "mineral"   ]
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter([
        { column: "numbers", gte: 10, lt: "30" }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "10", "dog",      "animal"  ],
            [ "20", "calendar", "mineral" ]
        ]);
    }).clearFilters().applyFilter([
        { column: "category", ne: "mineral" },
        { column: "letters", gt: "b", lte: "dog" }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "2",  "banana",       "vegetable" ],
            [ 3,    "cat",          "animal"    ],
            [ "10", "dog",          "animal"    ],
            [ 100,  "bandersnatch", "animal"    ]
        ]);
    }).clearFilters().applyFilter([
        { column: "numbers", regex: /1/ },
        { column: "category", eq: "animal" }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "10", "dog",          "animal" ],
            [  100, "bandersnatch", "animal" ]
        ]);

        start();
    }).finish();
});

asyncTest("apply filter (complex, matchAll)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter([
        { columns: [ "column_b", "column_c" ], gte: "gum" }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }).clearFilters().applyFilter([
        { columns: [ "column_b", "column_c" ], gte: "gum", matchAll: true }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
        ]);

        start();
    }).finish();
});

asyncTest("apply filter (complex, empty filter does nothing)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter([ { } ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
        ]);

        start();
    }).finish();
});

asyncTest("apply filter (complex, columns can be string or array)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter([
        { columns: [ "column_a", "column_b" ], regex: /p/ }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }).clearFilters().applyFilter([
        { columns: "column_a", regex: /p/ }
    ]).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
        ]);

        start();
    }).finish();
});

asyncTest("search (apply complex filters)", function () {
    expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        deepEqual(rows, [ [ "dog" ] ]);
        start();
    }, [
        { columns: [ "column_b", "column_c" ], regex: /s/ },
        { regex: /at/ }
    ], { returnColumns: "column_c" }).finish();
});

asyncTest("search (fromRow)", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        deepEqual(rows, [
            [ "banana", "piano", "gum" ],
        ]);
    }, /p/, { fromRow: 1, limit: 1 }).search(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
        ]);

        start();
    }, /p/, { limit: 1 }).finish();
});

asyncTest("search (allRows)", function () {
    expect(3);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter(/asdf/).getRows(function (rows) {
        deepEqual(rows, [ ]);
    }).search(function (rows) {
        deepEqual(rows, [ ]);
    }, /p/).search(function (rows) {
        deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
        ]);

        start();
    }, /p/, { allRows: true }).finish();
});

asyncTest("search (getDistinct)", function () {
    expect(2);

    var numbers = { name: "numbers", sortType: "num" };

    var dataset = [
        [ numbers, "letters",      "category"  ],

        [ 1,       "apple",        "vegetable" ],
        [ "2",     "banana",       "vegetable" ],
        [ 3,       "cat",          "animal"    ],
        [ "10",    "dog",          "animal"    ],
        [ "20",    "calendar",     "mineral"   ],
        [ "30",    "ananas",       "vegetable" ],
        [ 100,     "bandersnatch", "animal"    ],
        [ "200",   "gorilla",      "animal"    ],
        [ 300,     "rock",         "mineral"   ]
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        deepEqual(rows, [
            [ 1,       "apple",        "vegetable" ],
            [ "2",     "banana",       "vegetable" ],
            [ 3,       "cat",          "animal"    ],
            [ "10",    "dog",          "animal"    ],
            [ "20",    "calendar",     "mineral"   ],
            [ "30",    "ananas",       "vegetable" ],
            [ 100,     "bandersnatch", "animal"    ],
            [ "200",   "gorilla",      "animal"    ],
            [ 300,     "rock",         "mineral"   ]
        ]);
    }, /a/, { getDistinct: true }).search(function (rows) {
        deepEqual(rows, [
            [ "vegetable" ],
            [ "animal"    ],
            [ "mineral"   ]
        ]);

        start();
    }, /a/, { getDistinct: true, columns: "category" }).finish();
});

asyncTest("sort by number allows scientific notation", function () {
    expect(1);

    var dataset = [
        [ { name: "numbers", sortType: "num" } ],

        [ "2.34e-5" ],
        [ "1.23e5"  ]
    ];

    var dw = new DataWorker(dataset);

    dw.sort("numbers").getRows(function (rows) {
        deepEqual(rows, [
            [ "2.34e-5" ],
            [ "1.23e5"  ]
        ]);

        start();
    }).finish();
});

asyncTest("removeColumns keeps the isVisible flags", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "piano",    "dog"      ],
        [ "banana",   "tissue",   "gum"      ]
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter(/o/, "column_b").getRows(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "piano",  "dog"   ]
        ]);
    }).removeColumns("column_c").getRows(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin" ],
            [ "cat",   "piano"  ]
        ]);

        start();
    }).finish();
});

asyncTest("column filters now support inverse regex (\"!regex\")", function () {
    expect(2);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "piano",    "dog"      ],
        [ "banana",   "tissue",   "gum"      ]
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter({ column: "column_b", regex: /o/ }).getRows(function (rows) {
        deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "piano",  "dog"   ]
        ]);
    }).clearFilters().applyFilter({ column: "column_b", "!regex": /o/ }).getRows(function (rows) {
        deepEqual(rows, [
            [ "banana", "tissue", "gum" ]
        ]);

        start();
    }).finish();
});
