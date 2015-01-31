QUnit.module("DataWorker (Local Data)");

QUnit.test("construct (simple columns)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    assert.ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
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
    }).finish(done);
});

QUnit.test("construct (force single-threaded execution)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    dataset.forceSingleThread = true;

    var d = new DataWorker(dataset);

    assert.ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
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
    }).finish(done);
});

QUnit.test("construct (complex columns)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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

    assert.ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
        assert.deepEqual(columns, {
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
    }).finish(done);
});

QUnit.test("apply filter (simple, unrestricted)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (simple, column-restricted, single column, found)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/apple/, "column_a").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (simple, column-restricted, single column, not found)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/apple/, "column_b").getRows(function (result) {
        assert.deepEqual(result, []);
    }).finish(done);
});


QUnit.test("apply filter (simple, column-restricted, multi-column, found)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
     .getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power", "apple"  ]
        ]);
     }).finish(done);
});

QUnit.test("apply filter (simple, column-restricted, multi-column, not found)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/piano/, "column_a", "column_c").getRows(function (result) {
        assert.deepEqual(result, []);
    }).finish(done);
});

QUnit.test("apply filter (complex, single)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, multi)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "cat",         "piano",      "gum" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, ignores column not found)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "cat",         "piano",      "gum" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (simple, multiple filters)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/m/).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "star"  ]
        ]);
    }).applyFilter(/e/).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power",  "star"  ]
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, multiple filters)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat", "tissue", "dog" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex then simple, multiple filters)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).applyFilter(/e/).getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat", "tissue", "dog" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (simple then complex, multiple filters)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat", "tissue", "dog" ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, single filter, multiple columns)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    ).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power",  "apple" ]
        ]);
    }).finish(done);
});

QUnit.test("clear filter", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.applyFilter(/apple/).clearFilters();

    d.getRows(function(result) {
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }).finish(done);
});

QUnit.test("filter (hard)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.filter(/apple/);

    d.getRows(function(result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("filter (hard, complex)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.filter({ column: "column_a", ne: "apple" });

    d.getRows(function(result) {
        assert.deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }).clearFilters().getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }).finish(done);
});

QUnit.test("limit", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.applyLimit(2).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue", "dog"   ]
        ]);
    }).finish(done);
});

QUnit.test("limit (hard)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.limit(2).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue", "dog"   ]
        ]);
    }).finish(done);
});

QUnit.test("remove columns", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(columns, {
            column_a : {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_a",
                name      : "column_a",
                isVisible : true,
                index     : 0
            }
        });
        assert.deepEqual(rows, [
            [ "apple"  ],
            [ "cat"    ],
            [ "banana" ],
            [ "gummy"  ]
        ]);
    }).finish(done);
});

QUnit.test("sort (alpha)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.sort("column_b").getRows(function (result) {
        assert.deepEqual(result, [
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "apple" ],
            [ "cat",    "tissue", "dog"   ],
            [ "apple",  "violin", "music" ]
        ]);
    }).finish(done);
});

// FIXME: localeCompare doesn"t seem to work properly in Web Worker threads.
//QUnit.test("sort (locale-considering alpha)", function (assert) {
//    assert.expect(1);
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

//    d.sort("column_a").getRows(function (result) {
//        assert.deepEqual(result, [
//            [ "banana",      "piano",      "gum" ],
//            [ "gummy",       "power",     "star" ],
//            [ "résumé",     "tissue",      "dog" ],
//            [ "rip",        "violin",    "music" ]
//        ]);
//    }).finish(done);
//});

QUnit.test("sort (reverse alpha)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",    "apple" ]
    ];

    var d = new DataWorker(dataset);
    d.sort("-column_b").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "gummy",  "power",  "apple" ],
            [ "banana", "piano",  "gum"   ]
        ]);
    }).finish(done);
});

QUnit.test("sort (num)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    d.sort("column_c").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin",  8 ],
            [ "banana", "piano", 45 ],
            [ "gummy",  "power", 82 ],
            [ "cat",   "tissue", 85 ]
        ]);
    }).finish(done);
});

QUnit.test("sort (num w/ decimals)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    d.sort("column_c").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin",  "8.0" ],
            [ "banana", "piano", "45.0" ],
            [ "gummy",  "power", "82.0" ],
            [ "cat",   "tissue", "85.0" ]
        ]);
    }).finish(done);
});

QUnit.test("sort (reverse num)", function (assert) {
    assert.expect(1);

    var done = assert.async();
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
    d.sort("-column_c").getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",   "tissue", 85 ],
            [ "gummy",  "power", 82 ],
            [ "banana", "piano", 45 ],
            [ "apple", "violin",  8 ]
        ]);
    }).finish(done);
});

QUnit.test("sort (multi-column)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    d.sort("column_a", "column_c").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin",  5 ],
            [ "banana", "piano", 45 ],
            [ "cat",   "tissue", 85 ],
            [ "cat",    "power", 98 ]
        ]);
    }).finish(done);
});

QUnit.test("paginate (set page)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "car",        "screen",    "phone" ],
            [ "sign",        "bagel",    "chips" ]
        ]);
    }).finish(done);
});

QUnit.test("paginate (set 1st page)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
    }).finish(done);
});

QUnit.test("paginate (set 0th page)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
    }).finish(done);
});

QUnit.test("paginate (set negative page)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
    }).finish(done);
});

QUnit.test("paginate (next page)", function (assert) {
    assert.expect(4);

    var done = assert.async();

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
        assert.deepEqual(page1, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        assert.deepEqual(page2, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ]
        ]);
        assert.deepEqual(page3, [
            [ "car", "screen", "phone" ],
            [ "sign", "bagel", "chips" ]
        ]);
        assert.deepEqual(page4, page3);
    });

    d.getNextPage(function (result) {
        page1 = result;
    }).getNextPage(function (result) {
        page2 = result;
    }).getNextPage(function (result) {
        page3 = result;
    }).getNextPage(function (result) {
        page4 = result;
    }).render().finish(done);
});

QUnit.test("paginate (previous page)", function (assert) {
    assert.expect(6);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ]
        ]);
        assert.equal(pageNum, 2);
    }).getPreviousPage(function (rows, pageNum) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        assert.equal(pageNum, 1);
    }).getPreviousPage(function (rows, pageNum) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        assert.equal(pageNum, 1);
    }).finish(done);
});

QUnit.test("paginate (get current page)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(page, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ],
        ]);
    }).finish(done);
});

QUnit.test("paginate (get specific page)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(page, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ],
        ]);
    }, 2).finish(done);
});

QUnit.test("paginate (getPage passes rows and current page number to callback)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(page, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ],
        ]);

        assert.equal(pageNum, 2);
    }, 2).finish(done);
});

QUnit.test("paginate (request after last page returns last page)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(page, [
            [ "car",  "screen", "phone" ],
            [ "sign", "bagel",  "chips" ]
        ]);

        assert.equal(pageNum, 3);
    }, 7).finish(done);
});

QUnit.test("paginate (only returns visible rows)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(page, [
            [ "banana", "piano",  "gum"   ],
            [ "car",    "screen", "phone" ],
        ]);

        assert.equal(pageNum, 2);
    }, 2).finish(done);
});

QUnit.test("paginate (next page when already on last page keeps dataset on last page", function (assert) {
    assert.expect(8);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue",   "dog" ]
        ]);
        assert.equal(pageNum, 1);
    }).getNextPage(function (rows, pageNum) {
        assert.deepEqual(rows, [
            [ "banana", "piano",   "gum" ],
            [ "gummy",  "power", "apple" ]
        ]);
        assert.equal(pageNum, 2);
    }).getNextPage(function (rows, pageNum) {
        assert.deepEqual(rows, [
            [ "car", "screen", "phone" ],
            [ "sign", "bagel", "chips" ]
        ]);
        assert.equal(pageNum, 3);
    }).getNextPage(function (rows, pageNum) {
        assert.deepEqual(rows, [
            [ "car", "screen", "phone" ],
            [ "sign", "bagel", "chips" ]
        ]);
        assert.equal(pageNum, 3);
    }).finish(done);
});

QUnit.test("paginate (get number of pages)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.equal(numPages, 3);
    }).finish(done);
});

QUnit.test("append", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    d.append(dataset2).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",    "apple" ],
            [ "car",        "screen",    "phone" ],
            [ "sign",        "bagel",    "chips" ]
        ]);
    }).finish(done);
});

QUnit.test("append DataWorker", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
    d1.append(d2).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",    "apple" ],
            [ "car",        "screen",    "phone" ],
            [ "sign",        "bagel",    "chips" ]
        ]);
        d2.finish();
    }).finish(done);
});

QUnit.test("failed append (columns not the same)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.equal(
            error,
            "Cannot append dataset (columns do not match):\n\t"
            + "column_a, column_b, column_c\n\t\t"
            + "VS\n\t"
            + "column_a, column_b, column_d"
        );
    });

    d.append(dataset2).finish(done);
});

QUnit.test("failed append (different number of columns)", function (assert) {
    var done = assert.async();

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
        assert.equal(
            error,
            "Cannot append dataset (columns do not match):\n\t"
            + "column_a, column_b, column_c\n\t\t"
            + "VS\n\t"
            + "column_a, column_b"
        );
    });

    d.append(dataset2).finish(done);
});

QUnit.test("join (inner join on single field)", function (assert) {
    assert.expect(2);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.join(d2, "column_a", "column_d");

    d1.sort("column_a", "column_f").getColumnsAndRecords(function (columns, rows) {
        assert.deepEqual(columns, {
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
        assert.deepEqual(rows, [
            [ "apple", "violin", "music", "apple",    "screen", "phone" ],
            [ "banana", "piano", "gum",  "banana",     "power", "apple" ],
            [ "cat",   "tissue", "dog",     "cat",     "bagel", "chips" ],
            [ "cat",   "tissue", "dog",     "cat", "amsterdam", "drops" ]
        ]);

        d2.finish(done2);
    }).finish(done1);
});

QUnit.test("join (left outer join on single field)", function (assert) {
    assert.expect(1);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.join(d2, "column_a", "column_d", "left");

    d1.sort("column_a", "column_f").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",   "violin", "music",  "apple",    "screen", "phone" ],
            [ "banana",   "piano",   "gum", "banana",     "power", "apple" ],
            [ "cat",     "tissue",   "dog",    "cat",     "bagel", "chips" ],
            [ "dump", "amsterdam", "drops",       "",          "",      "" ],
        ]);
        d2.finish(done2);
    }).finish(done1);
});

QUnit.test("join (right outer join on single field", function (assert) {
    assert.expect(1);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.join(d2, "column_a", "column_d", "right");

    d1.sort("column_a", "column_f").getRows(function (result) {
        assert.deepEqual(result, [
            [ "",              "",      "",    "car",      "nuts",  "axes" ],
            [ "apple",   "violin", "music",  "apple",    "screen", "phone" ],
            [ "banana",   "piano",   "gum", "banana",     "power", "apple" ],
            [ "cat",     "tissue",   "dog",    "cat",     "bagel", "chips" ]
        ]);
        d2.finish(done2);
    }).finish(done1);
});

QUnit.test("join (inner join on multiple fields)", function (assert) {
    assert.expect(1);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.join(
        d2,
        [ "column_a", "column_b" ],
        [ "column_d", "column_e" ]
    );

    d1.getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat", "tissue", "dog", "cat", "tissue", "drops" ]
        ]);
        d2.finish(done2);
    }).finish(done1);
});

QUnit.test("failed join (unknown join type)", function (assert) {
    assert.expect(1);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.onError(function (error) {
        assert.equal(error, "Unknown join type.");

        d1.finish(done1);
        d2.finish(done2);
    });

    d1.join(d2, "column_a", "column_d", "crazy");
});

QUnit.test("failed join (columns with same name)", function (assert) {
    assert.expect(1);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.onError(function (error) {
        assert.equal(error, "Column names overlap.");

        d1.finish(done1);
        d2.finish(done2);
    });

    d1.join(d2, "column_a", "column_d");
});

QUnit.test("prepend column names", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(columns, {
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
    }).finish(done);
});

QUnit.test("alter column name", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnName("column_a", "a_column");

    d.getColumns(function (columns) {
        assert.deepEqual(columns, {
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
    }).finish(done);
});

QUnit.test("alter column name (fails if changing to already existing column name)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).onError(function (error) {
        assert.equal(error, "Column column_b already exists in the dataset.");
    });

    d.alterColumnName("column_a", "column_b").finish(done);
});

QUnit.test("alter column sort type", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnSortType("column_a", "random");

    d.getColumns(function (columns) {
        assert.equal(columns["column_a"]["sortType"], "random");
    }).finish(done);
});

QUnit.test("alter column aggregate type", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnAggregateType("column_a", "random");

    d.getColumns(function (columns) {
        assert.equal(columns["column_a"]["aggType"], "random");
    }).finish(done);
});

QUnit.test("alter column title", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).alterColumnTitle("column_a", "random");

    d.getColumns(function (columns) {
        assert.equal(columns["column_a"]["title"], "random");
    }).finish(done);
});

QUnit.test("group (single field sum)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", 576 ],
            [ "cat",   663 ],
            [ "gummy",  34 ]
        ]);
    }).finish(done);
});

QUnit.test("group (single field max)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", 453 ],
            [ "cat",   663 ],
            [ "gummy",  34 ]
        ]);
    }).finish(done);
});

QUnit.test("group (single field min)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", 123 ],
            [ "cat",   663 ],
            [ "gummy",  34 ]
        ]);
    }).finish(done);
});

QUnit.test("group (multi-field)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",  "piano",  218 ],
            [ "apple",  "violin", 453 ],
            [ "cat",    "tissue", 663 ],
            [ "gummy",   "power", 802 ]
        ]);
    }).finish(done);
});

QUnit.test("apply filter operates on partitioned datasets as well", function (assert) {
    assert.expect(4);

    var done = assert.async();

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
        assert.deepEqual(
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
        assert.deepEqual(
            bananaPartition,
            [
                [ "banana",      "piano",      "gum" ],
            ]
        );
        assert.deepEqual(
            catPartition,
            [ ]
        );
        assert.deepEqual(
            gumPartition,
            [
                [ "gum",           "gun",     "trix" ]
            ]
        );
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
            d.render().finish(done);
        } else {
            setTimeout(wait, 0);
        }
    })();
});

QUnit.test("clear filter operates on partitioned datasets as well", function (assert) {
    assert.expect(4);

    var done = assert.async();

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
        assert.deepEqual(
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
        assert.deepEqual(
            bananaPartition,
            [
                [ "banana",      "piano",      "gum" ],
                [ "banana",   "eyedrops",      "tie" ]
            ]
        );
        assert.deepEqual(
            catPartition,
            [
                [ "cat",       "nothing",      "dog" ]
            ]
        );
        assert.deepEqual(
            gumPartition,
            [
                [ "gum",           "gun",     "trix" ]
            ]
        );
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
            d.render().finish(done);
        } else {
            setTimeout(wait, 0);
        }
    })();
});

QUnit.test("partitioned datasets obeys hidden columns", function (assert) {
    assert.expect(4);

    var done = assert.async();

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
        assert.deepEqual(
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
        assert.deepEqual(
            bananaPartition,
            [
                [    "piano",      "gum" ],
                [ "eyedrops",      "tie" ]
            ]
        );
        assert.deepEqual(
            catPartition,
            [
                [ "nothing",      "dog" ]
            ]
        );
        assert.deepEqual(
            gumPartition,
            [
                [ "gun",     "trix" ]
            ]
        );
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
            d.render().finish(done);
        } else {
            setTimeout(wait, 0);
        }
    })();
});

QUnit.test("get partitioned (single field)", function (assert) {
    assert.expect(5);

    var done = assert.async();

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
        assert.deepEqual(partitionKeys.sort(), [
            [ "apple"  ],
            [ "banana" ],
            [ "cat"    ],
            [ "gummy"  ]
        ]);

        assert.deepEqual(
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

        assert.deepEqual(
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

        assert.deepEqual(
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

        assert.deepEqual(gummyPartition, [
            [ "gummy", "power", "star" ]
        ]);
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
            d.render().finish(done);
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
});

QUnit.test("get partitioned (multi-field)", function (assert) {
    assert.expect(7);

    var done = assert.async();

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
        assert.deepEqual(partitionKeys.sort(), [
            [ "apple", "trance" ],
            [ "apple", "violin" ],
            [ "banana", "piano" ],
            [ "cat",      "soy" ],
            [ "cat",   "tissue" ],
            [ "gummy", "power"  ]
        ]);

        assert.deepEqual(appleTrancePartition, [
            [ "apple", "trance", "camaro" ]
        ]);

        assert.deepEqual(
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

        assert.deepEqual(
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

        assert.deepEqual(catSoyPartition, [
            [ "cat", "soy",  "blender" ]
        ]);

        assert.deepEqual(catTissuePartition, [
            [ "cat", "tissue", "dog" ]
        ]);

        assert.deepEqual(gummyPowerPartition, [
            [ "gummy", "power", "star" ]
        ]);
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
            d.render().finish(done);
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
});

QUnit.test('get partition (multi-field w/ nulls)', function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" },
        ],

        [ "apple",      "violin",    "music" ],
        [ "cat",            null,      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ],
        [ "apple",          null,   "camaro" ],
        [ "cat",            null,  "blender" ],
        [ "cat",        "potato",  "blender" ],
        [ "banana",      "piano",      "tie" ],
        [ "apple",      "violin",      "key" ]
    ];

    var d = new DataWorker(dataset).partition("column_a", "column_b");

    d.getPartitionKeys(function (partitionKeys) {
        assert.deepEqual(partitionKeys.sort(), [
            [ "apple",       "" ],
            [ "apple", "violin" ],
            [ "banana", "piano" ],
            [ "cat",         "" ],
            [ "cat",   "potato" ],
            [ "gummy", "power"  ]
        ]);
    })

    d.getPartitioned(function (partition) {
        assert.deepEqual(partition, [
            [ "cat", null,     "dog" ],
            [ "cat", null, "blender" ]
        ]);
    }, "cat", null);

    d.finish(done);
});

QUnit.test('get partition (empty partition)', function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" },
        ],

        [ "apple", "violin", "music" ],
    ];

    var d = new DataWorker(dataset).partition("column_a", "column_b");

    d.getPartitionKeys(function (partitionKeys) {
    })

    d.getPartitioned(function (partition) {
        assert.deepEqual(partition, []);
    }, "non", "existent");

    d.finish(done);
});

QUnit.test("clone", function (assert) {
    assert.expect(3);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    var done1 = assert.async();

    d.clone(function (clone) {
        assert.ok(clone instanceof DataWorker);

        var done2 = assert.async();

        clone.getColumnsAndRecords(function (columns, records) {
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
        }).finish(done2);
    }).finish(done1);
});

QUnit.test("sort partition", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        [ "apple",         "gum",     "trix" ]
    ];

    var d = new DataWorker(dataset).partition("column_a").sortPartition("apple", "column_c");

    d.getPartitioned(function (partition) {
        assert.deepEqual(
            partition,
            [
                [ "apple",      "violin",    "music" ],
                [ "apple",         "gum",     "trix" ],
                [ "apple",         "gum",   "wallet" ]
            ]
        );
    }, "apple").finish(done);
});

QUnit.test("get rows (all)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }).finish(done);
});

QUnit.test("get rows (specify done)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }, 2).finish(done);
});

QUnit.test("get rows (specify end)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ]
        ]);
    }, undefined, 1).finish(done);
});

QUnit.test("get rows (specify done and end)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ]
        ]);
    }, 1, 2).finish(done);
});

QUnit.test("get rows (specify a too-large end)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }, 1, 10).finish(done);
});

QUnit.test("get rows (specify columns)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",     "dog" ],
            [ "banana",  "gum" ],
            [ "gummy",  "star" ]
        ]);
    }, 1, 10, "column_a", "column_c").finish(done);
});

QUnit.test("get rows (specify columns as array)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "cat",     "dog" ],
            [ "banana",  "gum" ],
            [ "gummy",  "star" ]
        ]);
    }, 1, 10, [ "column_a", "column_c" ]).finish(done);
});

QUnit.test("get rows (specify out-of-order columns)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function (result) {
        assert.deepEqual(result, [
            [ "dog", "cat" ]
        ]);
    }, 1, 1, "column_c", "column_a").finish(done);
});

QUnit.test("get number of records", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getNumberOfRecords(function (result) {
        assert.equal(result, 4);
    }).finish(done);
});

QUnit.test("getRows obeys applied filter", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("getColumnsAndRecords obeys applied filter", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("setDecimalMarkCharacter", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "1.435" ],
            [ "4.560" ],
            [ "2,345" ],
            [ "3,600" ]
        ]);
     }).setDecimalMarkCharacter(",")
     .sort("column_a")
     .getColumnsAndRecords(function (columns, rows) {
        assert.deepEqual(rows, [
            [ "2,345" ],
            [ "3,600" ],
            [ "1.435" ],
            [ "4.560" ]
        ]);
     }).finish(done);
});

QUnit.test("getDistinctConsecutiveRows", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "abc", 0, 2 ],
            [ "def", 3, 3 ],
            [ "ghi", 4, 5 ],
            [ "def", 6, 7 ]
        ]);
     }, "column_a").getDistinctConsecutiveRows(function (rows) {
        assert.deepEqual(rows, [
            [ "123", 0, 0 ],
            [ "456", 1, 1 ],
            [ "789", 2, 2 ],
            [ "123", 3, 4 ],
            [ "456", 5, 6 ],
            [ "789", 7, 7 ]
        ]);
     }, "column_b").finish(done);
});

QUnit.test("extraColumnInfoGetsPassedAlong", function (assert) {
    assert.expect(2);

    var done = assert.async();

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

    assert.ok(d instanceof DataWorker);

    d.getColumns(function (columns) {
        assert.deepEqual(columns, {
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
    }).finish(done);
});

QUnit.test("hide columns (single)", function (assert) {
    assert.expect(4);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.hideColumns("column_a").getColumnsAndRecords(function (columns, records) {
        assert.deepEqual(columns, {
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

        assert.deepEqual(records, [
            [ "violin",    "music" ],
            [ "tissue",      "dog" ],
            [  "piano",      "gum" ],
            [  "power",     "star" ]
        ]);
    }).getAllColumnsAndAllRecords(function (columns, records) {
        assert.deepEqual(columns, {
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

        assert.deepEqual(records, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",     "star" ]
        ]);
    }).finish(done);
});

QUnit.test("hide columns (multi)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns("column_a", "column_c");

    d.getColumnsAndRecords(function (columns, records) {
        assert.deepEqual(columns, {
            column_b: {
                sortType  : "alpha",
                aggType   : "max",
                title     : "column_b",
                name      : "column_b",
                isVisible : true,
                index     : 0
            }
        });

        assert.deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);
    }).finish(done);
});

QUnit.test("hide column that does not exist does not error out", function (assert) {
    assert.expect(0);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset)
                              .hideColumns("column_d")
                              .finish(done);
});

QUnit.test("hide columns (regex)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "notcolumn_b", "column_c" ],

        [ "apple",         "violin",    "music" ],
        [ "cat",           "tissue",      "dog" ],
        [ "banana",         "piano",      "gum" ],
        [ "gummy",          "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns(/^column/);

    d.getColumnsAndRecords(function (columns, records) {
        assert.deepEqual(columns, {
            notcolumn_b : {
                sortType  : "alpha",
                aggType   : "max",
                title     : "notcolumn_b",
                name      : "notcolumn_b",
                isVisible : true,
                index     : 0
            }
        });

        assert.deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);
    }).finish(done);
});

QUnit.test("hide columns (regex, w/ flags)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "notcolumn_b", "COLumn_c" ],

        [ "apple",         "violin",    "music" ],
        [ "cat",           "tissue",      "dog" ],
        [ "banana",         "piano",      "gum" ],
        [ "gummy",          "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns(/^column/i);

    d.getColumnsAndRecords(function (columns, records) {
        assert.deepEqual(columns, {
            notcolumn_b : {
                sortType  : "alpha",
                aggType   : "max",
                title     : "notcolumn_b",
                name      : "notcolumn_b",
                isVisible : true,
                index     : 0
            }
        });

        assert.deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);
    }).finish(done);
});

QUnit.test("hide columns (by column property)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [
            {
                name: "column_a",
                aggType: "max",
                sortType: "alpha",
                title: "Column A",
                shouldHide: "asdf"
            },
            {
                name: "column_b",
                aggType: "max",
                sortType: "alpha",
                title: "Column B",
                shouldHide: "zxcv"
            },
            {
                name: "column_c",
                aggType: "min",
                sortType: "alpha",
                title: "Column C",
                shouldHide: "asdf"
            }
        ],

        [ "apple",         "violin",    "music" ],
        [ "cat",           "tissue",      "dog" ],
        [ "banana",         "piano",      "gum" ],
        [ "gummy",          "power",     "star" ]
    ];

    var d = new DataWorker(dataset).hideColumns({ property: "shouldHide", value: "asdf" });

    d.getColumnsAndRecords(function (columns, records) {
        assert.deepEqual(columns, {
            column_b : {
                sortType   : "alpha",
                aggType    : "max",
                title      : "Column B",
                name       : "column_b",
                isVisible  : true,
                index      : 0,
                shouldHide : "zxcv"
            }
        });

        assert.deepEqual(records, [
            [ "violin" ],
            [ "tissue" ],
            [  "piano" ],
            [  "power" ]
        ]);
    }).finish(done);
});

QUnit.test(
    "hide columns (by column property, does not affect columns w/o the property)",
    function (assert) {
        assert.expect(2);

        var done = assert.async();

        var dataset = [
            [
                {
                    name: "column_a",
                    aggType: "max",
                    sortType: "alpha",
                    title: "Column A",
                    shouldHide: "asdf"
                },
                {
                    name: "column_b",
                    aggType: "max",
                    sortType: "alpha",
                    title: "Column B",
                    shouldHide: "zxcv"
                },
                {
                    name: "column_c",
                    aggType: "min",
                    sortType: "alpha",
                    title: "Column C"
                }
            ],

            [ "apple",         "violin",    "music" ],
            [ "cat",           "tissue",      "dog" ],
            [ "banana",         "piano",      "gum" ],
            [ "gummy",          "power",     "star" ]
        ];

        var d = new DataWorker(dataset).hideColumns({ property: "shouldHide", value: "asdf" });

        d.getColumnsAndRecords(function (columns, records) {
            assert.deepEqual(columns, {
                column_b : {
                    sortType   : "alpha",
                    aggType    : "max",
                    title      : "Column B",
                    name       : "column_b",
                    isVisible  : true,
                    index      : 0,
                    shouldHide : "zxcv"
                },
                column_c : {
                    sortType   : "alpha",
                    aggType    : "min",
                    title      : "Column C",
                    name       : "column_c",
                    isVisible  : true,
                    index      : 1
                }
            });

            assert.deepEqual(records, [
                [ "violin", "music" ],
                [ "tissue", "dog"   ],
                [  "piano", "gum"   ],
                [  "power", "star"  ]
            ]);
        }).finish(done);
    }
);

QUnit.test("getColumns respects hidden columns", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.hideColumns("column_b").getColumns(function (columns) {
        assert.deepEqual(Object.keys(columns), [ "column_a", "column_c" ]);
    }).finish(done);
});

QUnit.test("show columns", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
            }
        });

        assert.deepEqual(records, [
            [ "apple",      "violin" ],
            [ "cat",        "tissue" ],
            [ "banana",      "piano" ],
            [ "gummy",       "power" ]
        ]);
    }).finish(done);
});

QUnit.test("show column that does not exist does not error out", function (assert) {
    assert.expect(0);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset)
                              .showColumns("column_d")
                              .finish(done);
});

QUnit.test("show columns (regex)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(columns, {
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

        assert.deepEqual(records, [
            [ "apple",      "violin" ],
            [ "cat",        "tissue" ],
            [ "banana",      "piano" ],
            [ "gummy",       "power" ]
        ]);
    }).finish(done);
});

QUnit.test("hide all columns" , function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(columns, {});
        assert.deepEqual(records, [ [], [], [], [] ]);
    }).finish(done);
});

QUnit.test("show all columns", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
    }).finish(done);
});

QUnit.test("changes for \"on_\" functions are added to the queue by default", function (assert) {
    assert.expect(3);

    var done = assert.async();

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
        assert.equal(error, "Column column_b already exists in the dataset.");

        d.finish(done);
    });

    assert.equal(d._actionQueue._queueStack.length, 1);
    assert.equal(d._actionQueue._queueStack[0].length, 1);

    d.alterColumnName("column_a", "column_b");

    d._finishAction();
});

QUnit.test("changes for \"on_\" functions can happen immediately with a flag", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(error, "Column column_b already exists in the dataset.");

        d.finish(done);
    }, true);

    assert.equal(d._actionQueue._queueStack.length, 0);

    d.alterColumnName("column_a", "column_b");

    d._finishAction();
});

QUnit.test("add child rows", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(num, 12);
    });

    d.getRows(function (rows) {
        assert.deepEqual(rows, [
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
    }).finish(done);
});

QUnit.test("add child rows using another DataWorker object", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(num, 12);
    });

    d.getRows(function (rows) {
        assert.deepEqual(rows, [
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
    }).finish(done);
});

QUnit.test("children of invisible parents default to invisible", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(num, 5);
    });

    d.getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",  "violin",        "music"   ],
                [ "apple",  "fuji",          "red"     ],
                [ "apple",  "red delicious", "red"     ],
                [ "apple",  "granny smith",  "green"   ],
                [ "apple",  "honey crisp",   "yellow"  ]
        ]);
    }).finish(done);
});

QUnit.test("sorts children as subsets of parents, not as part of the whole dataset", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(num, 12);
    });

    d.sort("column_b").getRows(function (rows) {
        assert.deepEqual(rows, [
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
    }).finish(done);
});

QUnit.test("sorts children within parents when parents have ties", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(num, 12);
    });

    d.sort("column_b").getRows(function (rows) {
        assert.deepEqual(rows, [
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
    }).finish(done);
});

QUnit.test("multiple column sort with nulls in number columns", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var d = new DataWorker([
        [ "numeric_column", "alpha_column" ],

        [ null,             "xyz"          ],
        [ null,             "abc"          ],
        [ null,             "lmnop"        ],
        [ null,             "def"          ]
    ]);

    d.alterColumnSortType("numeric_column", "num");

    d.sort("numeric_column", "alpha_column").getRows(function (rows) {
        assert.deepEqual(rows, [
            [ null, "abc"   ],
            [ null, "def"   ],
            [ null, "lmnop" ],
            [ null, "xyz"   ]
        ]);
    }).finish(done);
});

QUnit.test("getDistinctConsecutiveRows with child rows just looks at parent data", function (assert) {
    assert.expect(5);

    var done = assert.async();

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
        assert.equal(num, 14);
    });

    d.alterColumnSortType("rank", "num")
     .alterColumnSortType("amount", "num")
     .sort("-amount", "rank", "side")
     .getRows(function (rows) {
        assert.deepEqual(rows, [
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
        assert.deepEqual(rows, [
            [ "One",    0,  3 ],
            [ "Two",    4,  5 ],
            [ "Four",   6, 10 ],
            [ "Three", 11, 13 ]
        ]);
     }, "name").getDistinctConsecutiveRows(function (rows) {
        assert.deepEqual(rows, [
            [ null, 0, 13 ]
        ]);
     }, "side").getDistinctConsecutiveRows(function (rows) {
        assert.deepEqual(rows, [
            [ 500,  0,  5 ],
            [ 300,  6, 10 ],
            [ 100, 11, 13 ]
        ]);
     }, "amount").finish(done);
});

QUnit.test("DataWorker works without Web Worker support (older browsers)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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

    d.applyFilter(/apple/, "column_a").getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("then() function lets you utilize DataWorker\"s action queue", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.equal(x, 10);
        x = numRows;
    }).then(function () {
        assert.equal(x, 4);
    }).finish(done);
});

QUnit.test("clearDataset", function (assert) {
    assert.expect(3);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getRows(function(result) {
        assert.deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "star"  ]
        ]);
    }).clearDataset().getColumns(function (result) {
        assert.deepEqual(result, {});
    }).getRows(function (result) {
        assert.deepEqual(result, []);
    }).finish(done);
});

QUnit.test("appending to empty dataset takes in new columns", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker([ [] ]).append(dataset);

    d.getRows(function(result) {
        assert.deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "star"  ]
        ]);
    }).finish(done);
});

QUnit.test("getHashedRows base case", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.getHashedRows(function (result) {
        assert.deepEqual(result, [
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
    }).finish(done);
});

QUnit.test("getPage can ask for specific columns", function (assert) {
    assert.expect(6);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ "gummy",       "power",     "star" ]
    ];

    var d = new DataWorker(dataset);

    d.paginate(3).getPage(function (page, pageNum) {
        assert.deepEqual(page, [
            [ "apple",  "violin" ],
            [ "cat",    "tissue" ],
            [ "banana", "piano"  ]
        ]);
        assert.equal(pageNum, 1);
    }, undefined, "column_a", "column_b").getPage(function (page, pageNum) {
        assert.deepEqual(page, [
            [ "gummy",  "star" ]
        ]);
        assert.equal(pageNum, 2);
    }, 2, [ "column_a", "column_c" ]).getPage(function (page, pageNum) {
        assert.deepEqual(page, [
            [ "violin" ],
            [ "tissue" ],
            [ "piano"  ]
        ]);
        assert.equal(pageNum, 1);
    }, 1, "column_b").finish(done);
});

QUnit.test("get/set summary rows", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "things", "stuffs", "foothings" ],
            [ "things", "stuffs", "foostuffs" ]
        ]);
    }).finish(done);
});

QUnit.test("get/set summary rows (set with an array of summary rows)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "things", "stuffs", "foothings" ],
            [ "things", "stuffs", "foostuffs" ]
        ]);
    }).finish(done);
});

QUnit.test("get summary rows (specify columns)", function (assert) {
    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "foothings", "things" ],
            [ "foostuffs", "things" ]
        ]);
    }, "column_c", "column_a").finish(done);
});

QUnit.test("apply filter (simple, column-restricted, multi-column, found, using array for column names)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
     .getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power", "apple"  ]
        ]);
     }).finish(done);
});

QUnit.test("search (simple)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);
    d.search(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "gummy", "power", "apple"  ]
        ]);
    }, /APPLE/i).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "gummy",  "power",  "apple" ]
        ]);
    }).finish(done);
});

QUnit.test("search (simple, only searches visible rows)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);
    d.applyFilter(/i/).getRows(function (result) {
        assert.deepEqual(result, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ]
        ]);
    }).search(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ]
        ]);
    }, "apple").finish(done);
});

QUnit.test("search (specify columns)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin" ]
        ]);
    }, /apple/, { columns: [ "column_a", "column_b" ] }).finish(done);
});

QUnit.test("search (specify columns, single column without array)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        assert.deepEqual(result, [
            [ "music" ],
            [ "gum"   ]
        ]);
    }, /u/, { columns: "column_c" }).finish(done);
});

QUnit.test("search (specify sort order)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        assert.deepEqual(result, [
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "apple",  "violin", "music" ]
        ]);
    }, /i/, { sortOn: [ "-column_a" ] }).finish(done);
});

QUnit.test("search (specify sort order, single column without array)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        assert.deepEqual(result, [
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
            [ "apple",  "violin", "music" ]
        ]);
    }, /i/, { sortOn: "-column_a" }).finish(done);
});

QUnit.test("search (limit number of results)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "tissue",   "dog"      ],
        [ "banana",   "piano",    "gum"      ],
        [ "gummy",    "power",    "apple"    ]
    ];

    var d = new DataWorker(dataset);

    d.search(function (result) {
        assert.deepEqual(result, [
            [ "apple", "violin", "music" ],
            [ "cat",   "tissue", "dog"   ]
        ]);
    }, /o/, { limit: 2 }).finish(done);
});

QUnit.test("search (sort on columns that aren't being fetched)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "banana", "gum"   ],
            [ "gummy",  "apple" ],
            [ "apple",  "music" ]
        ]);
    }, /[mu]{2}/, searchOptions).finish(done);
});

QUnit.test("search (search different columns than those being returned)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "violin" ],
            [ "piano"  ],
            [ "power"  ]
        ]);
    }, /(?:apple|gum)/, searchOptions).finish(done);
});

QUnit.test("search (return different columns than those being searched)", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        assert.deepEqual(result, [
            [ "apple",  "music" ],
            [ "banana", "gum"   ],
            [ "gummy",  "apple" ]
        ]);
    }, /p/, searchOptions).finish(done);
});

QUnit.test("two single-threaded dataworkers", function (assert) {
    assert.expect(2);

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

    var done1 = assert.async();
    var done2 = assert.async();

    d1.sort("column_a");
    d2.sort("column_c");

    d1.getRows(function (rows1) {
        assert.deepEqual(rows1, [
            [ "apple",      "violin",    "music" ],
            [ "banana",      "piano",      "gum" ],
            [ "cat",        "tissue",      "dog" ],
        ]);

        d2.getRows(function (rows2) {
            assert.deepEqual(rows2, [
                [ "gummy",       "power",    "apple" ],
                [ "sign",        "bagel",    "chips" ],
                [ "car",        "screen",    "phone" ]
            ]);
        }).finish(done2);
    }).finish(done1);
});

QUnit.test("applying filters to non-existant columns does not break dataworker", function (assert) {
    assert.expect(3);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter(/violin/, [ "column_b", "column_x" ]).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ]
        ]);
    }).clearDataset().applyFilter(/violin/, "column_b").getRows(function (rows) {
        assert.deepEqual(rows, []);
    }).append(dataset).applyFilter(/violin/, "column_b").getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ]
        ]);
    }, /violin/).finish(done);
});

QUnit.test("searching non-existant columns does not break dataworker", function (assert) {
    assert.expect(4);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        assert.deepEqual(rows, [
            [ "violin" ]
        ]);
    }, /v/, { columns: [ "column_b", "column_x" ] });

    dw.clearDataset().search(function (rows) {
        assert.deepEqual(rows, []);
    }, /v/, { columns: "column_b" });

    dw.search(function (rows) {
        assert.deepEqual(rows, []);
    }, /v/);

    dw.append(dataset).search(function (rows) {
        assert.deepEqual(rows, [
            [ "violin" ]
        ]);
    }, /v/, { columns: "column_b" }).finish(done);
});

QUnit.test("allow for blank column titles", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ { name: "abc", title: "" }, { name: "123" } ],

        [ "Alphabetic", "Numeric" ],
    ];

    var dw = new DataWorker(dataset);

    dw.getColumns(function (columns) {
        assert.equal(columns["abc"]["title"], "");
        assert.equal(columns["123"]["title"], "123");
    }).finish(done);
});

QUnit.test("apply filter (complex, gt/e, lt/e, eq, ne)", function (assert) {
    assert.expect(3);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "10", "dog",      "animal"  ],
            [ "20", "calendar", "mineral" ]
        ]);
    }).clearFilters().applyFilter([
        { column: "category", ne: "mineral" },
        { column: "letters", gt: "b", lte: "dog" }
    ]).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "2",  "banana",       "vegetable" ],
            [ 3,    "cat",          "animal"    ],
            [ "10", "dog",          "animal"    ],
            [ 100,  "bandersnatch", "animal"    ]
        ]);
    }).clearFilters().applyFilter([
        { column: "numbers", regex: /1/ },
        { column: "category", eq: "animal" }
    ]).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "10", "dog",          "animal" ],
            [  100, "bandersnatch", "animal" ]
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, matchAll)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }).clearFilters().applyFilter([
        { columns: [ "column_b", "column_c" ], gte: "gum", matchAll: true }
    ]).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, empty filter does nothing)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter([ { } ]).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "cat",    "tissue", "dog"   ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }).finish(done);
});

QUnit.test("apply filter (complex, columns can be string or array)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }).clearFilters().applyFilter([
        { columns: "column_a", regex: /p/ }
    ]).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
        ]);
    }).finish(done);
});

QUnit.test("search (apply complex filters)", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        assert.deepEqual(rows, [ [ "dog" ] ]);
    }, [
        { columns: [ "column_b", "column_c" ], regex: /s/ },
        { regex: /at/ }
    ], { returnColumns: "column_c" }).finish(done);
});

QUnit.test("search (fromRow)", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.search(function (rows) {
        assert.deepEqual(rows, [
            [ "banana", "piano", "gum" ],
        ]);
    }, /p/, { fromRow: 1, limit: 1 }).search(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
        ]);
    }, /p/, { limit: 1 }).finish(done);
});

QUnit.test("search (allRows)", function (assert) {
    assert.expect(3);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter(/asdf/).getRows(function (rows) {
        assert.deepEqual(rows, [ ]);
    }).search(function (rows) {
        assert.deepEqual(rows, [ ]);
    }, /p/).search(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",  "violin", "music" ],
            [ "banana", "piano",  "gum"   ],
        ]);
    }, /p/, { allRows: true }).finish(done);
});

QUnit.test("search (getDistinct)", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        assert.deepEqual(rows, [
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
        assert.deepEqual(rows, [
            [ "vegetable" ],
            [ "animal"    ],
            [ "mineral"   ]
        ]);
    }, /a/, { getDistinct: true, columns: "category" }).finish(done);
});

QUnit.test("sort by number allows scientific notation", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [ { name: "numbers", sortType: "num" } ],

        [ "2.34e-5" ],
        [ "1.23e5"  ]
    ];

    var dw = new DataWorker(dataset);

    dw.sort("numbers").getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "2.34e-5" ],
            [ "1.23e5"  ]
        ]);
    }).finish(done);
});

QUnit.test("removeColumns keeps the isVisible flags", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "piano",    "dog"      ],
        [ "banana",   "tissue",   "gum"      ]
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter(/o/, "column_b").getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "piano",  "dog"   ]
        ]);
    }).removeColumns("column_c").getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin" ],
            [ "cat",   "piano"  ]
        ]);
    }).finish(done);
});

QUnit.test("column filters now support inverse regex (\"!regex\")", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",    "violin",   "music"    ],
        [ "cat",      "piano",    "dog"      ],
        [ "banana",   "tissue",   "gum"      ]
    ];

    var dw = new DataWorker(dataset);

    dw.applyFilter({ column: "column_b", regex: /o/ }).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple", "violin", "music" ],
            [ "cat",   "piano",  "dog"   ]
        ]);
    }).clearFilters().applyFilter({ column: "column_b", "!regex": /o/ }).getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "banana", "tissue", "gum" ]
        ]);
    }).finish(done);
});

QUnit.test("complex record values", function (assert) {
    assert.expect(3);

    var done = assert.async();

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",                                      "violin",   "music"       ],
        [ { display: "<b>cat</b>", raw: "cat" },        "piano",    "dog"         ],
        [ { display: "<b>cat</b>", raw: "cat" },        "bike",     "solid"       ],
        [ "banana",                                     "tissue",   "gum"         ],
        [ "everything",                                 "is",       "awesome"     ],
        [ { display: "lion<3", raw: "lionhearted" },    "waves",    "Black Sheep" ]
    ];

    var d = new DataWorker(dataset);

    d.group("column_a").sort("column_a");

    d.getAllColumnsAndAllRecords(function (columns, rows) {
        assert.deepEqual(rows, [
            [ "apple",                                      "violin",   "music"       ],
            [ "banana",                                     "tissue",   "gum"         ],
            [ { display: "<b>cat</b>", raw: "cat" },        "piano",    "solid"       ],
            [ "everything",                                 "is",       "awesome"     ],
            [ { display: "lion<3", raw: "lionhearted" },    "waves",    "Black Sheep" ]
        ]);
    }, true);

    d.filter(/apple|cat|banana|lionhearted/, "column_a");

    d.getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",      "violin",   "music"       ],
            [ "banana",     "tissue",   "gum"         ],
            [ "<b>cat</b>", "piano",    "solid"       ],
            [ "lion<3",     "waves",    "Black Sheep" ]
        ]);
    });

    d.applyFilter(/apple|cat|banana/, "column_a");

    d.getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",      "violin",   "music"       ],
            [ "banana",     "tissue",   "gum"         ],
            [ "<b>cat</b>", "piano",    "solid"       ]
        ]);
    });

    d.finish(done);
});

QUnit.test("add child rows w/ complex record values", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var parentDataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ]
    ], childRows = [
        [ "apple",    "granny smith",  "green"    ],
        [ "apple",    "honey crisp",   "yellow"   ],

        [ { display: "<b>cat</b>", raw: "cat" }, "siamese", "tall" ]
    ];

    var d = new DataWorker(parentDataset).addChildRows(childRows, "column_a");

    d.getRows(function (rows) {
        assert.deepEqual(rows, [
            [ "apple",      "violin",    "music" ],
                [ "apple",    "granny smith",  "green"    ],
                [ "apple",    "honey crisp",   "yellow"   ],
            [ "cat",        "tissue",      "dog" ],
                [ "<b>cat</b>",      "siamese",       "tall"     ]
        ]);
    });

    d.finish(done);
});

QUnit.test("append maintains complex record values", function (assert) {
    assert.expect(1);

    var done = assert.async();

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
        [ { display: "<i>sign</i>", raw: "sign" },        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset1);

    d.append(dataset2).getAllColumnsAndAllRecords(function (columns, records) {
        assert.deepEqual(records, [
            [ "apple",      "violin",    "music" ],
            [ "cat",        "tissue",      "dog" ],
            [ "banana",      "piano",      "gum" ],
            [ "gummy",       "power",    "apple" ],
            [ "car",        "screen",    "phone" ],
            [ { display: "<i>sign</i>", raw: "sign" },        "bagel",    "chips" ]
        ]);
    }, true).finish(done);
});

QUnit.test("clone maintains complex record values", function (assert) {
    assert.expect(1);

    var dataset = [
        [ "column_a", "column_b", "column_c" ],

        [ "apple",      "violin",    "music" ],
        [ "cat",        "tissue",      "dog" ],
        [ "banana",      "piano",      "gum" ],
        [ { display: "<i>sign</i>", raw: "sign" },        "bagel",    "chips" ]
    ];

    var d = new DataWorker(dataset);

    var done1 = assert.async();

    d.clone(function (clone) {
        var done2 = assert.async();
        clone.getAllColumnsAndAllRecords(function (columns, records) {
            assert.deepEqual(records, [
                [ "apple",      "violin",    "music" ],
                [ "cat",        "tissue",      "dog" ],
                [ "banana",      "piano",      "gum" ],
                [ { display: "<i>sign</i>", raw: "sign" },        "bagel",    "chips" ]
            ]);
        }, true).finish(done2);
    }).finish(done1);
});

QUnit.test("partition dataset w/ complex record values", function (assert) {
    assert.expect(2);

    var done = assert.async();

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
        [ { display: "<i>apple</i>", raw: "apple" },         "gum",     "trix" ],
        [ "gum",           "gun",     "trix" ]
    ];

    var d = new DataWorker(dataset).partition("column_a");

    d.getPartitioned(function (partition) {
        assert.deepEqual(
            partition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ "apple",        "violin", "music"  ],
                [ "<i>apple</i>", "gum",    "trix"   ],
                [ "apple",        "gum",    "wallet" ]
            ]
        );
    }, "apple");

    d.applyFilter(/^apple$/, "column_a");

    d.getPartitioned(function (partition) {
        assert.deepEqual(
            partition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ "apple",        "violin", "music"  ],
                [ "<i>apple</i>", "gum",    "trix"   ],
                [ "apple",        "gum",    "wallet" ]
            ]
        );
    }, "apple");

    d.finish(done);
});

QUnit.test("hash dataset w/ complex record values", function (assert) {
    assert.expect(1);

    var done = assert.async();

    var dataset = [
        [
            { name: "column_a", sortType: "alpha", aggType: "max" },
            { name: "column_b", sortType: "alpha", aggType: "min" },
            { name: "column_c", sortType: "alpha", aggType: "min" }
        ],

        [ "banana",      "piano",      "gum" ],
        [ "apple",      "violin",    "music" ],
        [ "banana",   "eyedrops",      "tie" ],
        [ "apple",         "gum",   "wallet" ],
        [ { display: "<i>apple</i>", raw: "apple" },         "gum",     "trix" ],
    ];

    var d = new DataWorker(dataset);

    d.getHashOfDatasetByKeyColumns(function (hash) {
        assert.deepEqual(hash, {
            "apple": [
                [ "apple",      "violin",    "music" ],
                [ "apple",         "gum",   "wallet" ],
                [ { display: "<i>apple</i>", raw: "apple" },         "gum",     "trix" ],
            ],
            "banana": [
                [ "banana",      "piano",      "gum" ],
                [ "banana",   "eyedrops",      "tie" ],
            ]
        });
    }, "column_a").finish(done);
});

QUnit.test("join dataset w/ complex record values", function (assert) {
    assert.expect(1);

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
        [ { display: "<b>cat</b>", raw: "cat" },         "bagel",    "chips" ],
        [ { display: "<b>cat</b>", raw: "cat" },     "amsterdam",    "drops" ]
    ];

    var d1 = new DataWorker(dataset1);
    var d2 = new DataWorker(dataset2);

    var done1 = assert.async();
    var done2 = assert.async();

    d1.join(d2, "column_a", "column_d").sort("column_a", "column_f");

    d1.getAllColumnsAndAllRecords(function (columns, records) {
        assert.deepEqual(records, [
            [ "apple", "violin", "music", "apple",    "screen", "phone" ],
            [ "banana", "piano", "gum",  "banana",     "power", "apple" ],
            [ "cat",   "tissue", "dog", { display: "<b>cat</b>", raw: "cat" },     "bagel", "chips" ],
            [ "cat",   "tissue", "dog", { display: "<b>cat</b>", raw: "cat" }, "amsterdam", "drops" ]
        ]);

        d2.finish(done2);
    }, true);

    d1.finish(done1);
});
