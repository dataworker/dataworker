asyncTest('construct (simple columns)', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    ok(d instanceof JData);

    d.get_columns(function (columns) {
        deepEqual(columns, {
            column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_b',
                index     : 1
            },
            column_c: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_c',
                index     : 2
            }
        });

        start();
    });
});

asyncTest('construct (complex columns)', function () {
    expect(2);

    var dataset = [
        [
            {
                name: 'column_a',
                agg_type: 'max',
                sort_type: 'alpha',
                title: 'Column A'
            },
            {
                name: 'column_b',
                agg_type: 'max',
                sort_type: 'alpha',
                title: 'Column B'
            },
            {
                name: 'column_c',
                agg_type: 'min',
                sort_type: 'alpha',
                title: 'Column C'
            }
        ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    ok(d instanceof JData);

    d.get_columns(function (columns) {
        deepEqual(columns, {
            column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'Column A',
                index     : 0
            },
            column_b: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'Column B',
                index     : 1
            },
            column_c: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'Column C',
                index     : 2
            }
        });

        start();
    });
});

asyncTest('filter (unrestricted)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.filter(/apple/);

    d.get_dataset(function(result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
        ]);
        start();
    });
});

asyncTest('filter (column-restricted, single column, found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);
    d.filter(/apple/, 'column_a').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
        ]);
        start();
    });
});

asyncTest('filter (column-restricted, single column, not found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);
    d.filter(/apple/, 'column_b').get_dataset(function (result) {
        deepEqual(result, []);
        start();
    });
});


asyncTest('filter (column-restricted, multi-column, found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.filter(/apple/, 'column_a', 'column_c')
     .sort('column_a')
     .get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
            [ 'gummy', 'power', 'apple'  ]
        ]);
        start();
     });
});

asyncTest('filter (column-restricted, multi-column, not found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];


    var d = new JData(dataset);
    d.filter(/piano/, 'column_a', 'column_c').get_dataset(function (result) {
        deepEqual(result, []);
        start();
    });
});

test('limit', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.limit(2).get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
            [ 'cat',   'tissue', 'dog'   ]
        ]);
    });
});

asyncTest('remove columns', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.remove_columns('column_b', 'column_c')
     .get_columns_and_records(function (columns, rows) {
        deepEqual(columns, {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            }
        });
        deepEqual(rows, [
            [ 'apple'  ],
            [ 'cat'    ],
            [ 'banana' ],
            [ 'gummy'  ]
        ]);

        start();
    });
});

asyncTest('sort (alpha)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.sort('column_b').get_dataset(function (result) {
        deepEqual(result, [
            [ 'banana', 'piano',  'gum'   ],
            [ 'gummy',  'power',  'apple' ],
            [ 'cat',    'tissue', 'dog'   ],
            [ 'apple',  'violin', 'music' ]
        ]);
        start();
    });
});

asyncTest('sort (reverse alpha)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.sort('-column_b').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple',  'violin', 'music' ],
            [ 'cat',    'tissue', 'dog'   ],
            [ 'gummy',  'power',  'apple' ],
            [ 'banana', 'piano',  'gum'   ]
        ]);
        start();
    });
});

asyncTest('sort (num)', function () {
    expect(1);

    var dataset = [
        [
            {
                name: 'column_a',
                sort_type: 'alpha',
                agg_type: 'max'
            },
            {
                name: 'column_b',
                sort_type: 'alpha',
                agg_type: 'max'
            },
            {
                name: 'column_c',
                sort_type: 'num',
                agg_type: 'max'
            },
        ],

        [ 'apple',      'violin',          8 ],
        [ 'cat',        'tissue',         85 ],
        [ 'banana',      'piano',         45 ],
        [ 'gummy',       'power',         82 ]
    ];

    var d = new JData(dataset);
    d.sort('column_c').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin',  8 ],
            [ 'banana', 'piano', 45 ],
            [ 'gummy',  'power', 82 ],
            [ 'cat',   'tissue', 85 ]
        ]);
        start();
    });
});

asyncTest('sort (reverse num)', function () {
    expect(1);
    var dataset = [
        [
            {
                name: 'column_a',
                sort_type: 'alpha',
                agg_type: 'max'
            },
            {
                name: 'column_b',
                sort_type: 'alpha',
                agg_type: 'max'
            },
            {
                name: 'column_c',
                sort_type: 'num',
                agg_type: 'max'
            },
        ],

        [ 'apple',      'violin',          8 ],
        [ 'cat',        'tissue',         85 ],
        [ 'banana',      'piano',         45 ],
        [ 'gummy',       'power',         82 ]
    ];

    var d = new JData(dataset);
    d.sort('-column_c').get_dataset(function (result) {
        deepEqual(result, [
            [ 'cat',   'tissue', 85 ],
            [ 'gummy',  'power', 82 ],
            [ 'banana', 'piano', 45 ],
            [ 'apple', 'violin',  8 ]
        ]);
        start();
    });
});

asyncTest('sort (multi-column)', function () {
    expect(1);

    var dataset = [
        [
            {
                name: 'column_a',
                sort_type: 'alpha',
                agg_type: 'max'
            },
            {
                name: 'column_b',
                sort_type: 'alpha',
                agg_type: 'max'
            },
            {
                name: 'column_c',
                sort_type: 'num',
                agg_type: 'max'
            },
        ],

        [ 'apple',      'violin',          5 ],
        [ 'cat',        'tissue',         85 ],
        [ 'banana',      'piano',         45 ],
        [ 'cat',         'power',         98 ]
    ];

    var d = new JData(dataset);
    d.sort('column_a', 'column_c').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin',  5 ],
            [ 'banana', 'piano', 45 ],
            [ 'cat',   'tissue', 85 ],
            [ 'cat',    'power', 98 ]
        ]);
        start();
    });
});

asyncTest('paginate (set page)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2);

    d.render(function () {
        equal(d._current_page, 2);
        start();
    }).set_page(3).render();
});

asyncTest('paginate (set 1st page)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2);

    d.render(function () {
        equal(d._current_page, 0);
        start();
    }).set_page(1).render();
});

asyncTest('paginate (set 0th page)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2);

    d.render(function () {
        equal(d._current_page, 0);
        start();
    }).set_page(0).render();
});

asyncTest('paginate (set negative page)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2);

    d.render(function () {
        equal(d._current_page, 0);
        start();
    }).set_page(-1).render();
});

asyncTest('paginate (next page)', function () {
    expect(4);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var page1, page2, page3, page4;

    var d = new JData(dataset).paginate(2).render(function () {
        deepEqual(page1, [
            [ 'apple', 'violin', 'music' ],
            [ 'cat',   'tissue',   'dog' ]
        ]);
        deepEqual(page2, [
            [ 'banana', 'piano',   'gum' ],
            [ 'gummy',  'power', 'apple' ]
        ]);
        deepEqual(page3, [
            [ 'car', 'screen', 'phone' ],
            [ 'sign', 'bagel', 'chips' ]
        ]);
        deepEqual(page4, []);

        start();
    });

    d.get_next_page(function (result) {
        page1 = result;
    }).get_next_page(function (result) {
        page2 = result;
    }).get_next_page(function (result) {
        page3 = result;
    }).get_next_page(function (result) {
        page4 = result;
    }).render();
});

asyncTest('paginate (previous page)', function () {
    expect(4);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var page3, page2, page1, page0;

    var d = new JData(dataset).paginate(2).set_page(4).render(function () {
        deepEqual(page3, [
            [ 'car', 'screen', 'phone' ],
            [ 'sign', 'bagel', 'chips' ]
        ]);
        deepEqual(page2, [
            [ 'banana', 'piano',   'gum' ],
            [ 'gummy',  'power', 'apple' ]
        ]);
        deepEqual(page1, [
            [ 'apple', 'violin', 'music' ],
            [ 'cat',   'tissue',   'dog' ]
        ]);
        deepEqual(page0, [
            [ 'apple', 'violin', 'music' ],
            [ 'cat',   'tissue',   'dog' ]
        ]);

        start();
    });

    d.get_previous_page(function (result) {
        page3 = result;
    }).get_previous_page(function (result) {
        page2 = result;
    }).get_previous_page(function (result) {
        page1 = result;
    }).get_previous_page(function (result) {
        page0 = result;
    }).render();
});

asyncTest('paginate (get current page)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2).set_page(2);
    d.get_page(function (page) {
        deepEqual(page, [
            [ 'banana', 'piano',   'gum' ],
            [ 'gummy',  'power', 'apple' ],
        ]);
        start();
    });
});

asyncTest('paginate (get specific page)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2);
    var page = d.get_page(function (page) {
        deepEqual(page, [
            [ 'banana', 'piano',   'gum' ],
            [ 'gummy',  'power', 'apple' ],
        ]);
        start();
    }, 2);
});

asyncTest('append', function () {
    expect(1);

    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset1);
    d.append(dataset2).get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',    'apple' ],
            [ 'car',        'screen',    'phone' ],
            [ 'sign',        'bagel',    'chips' ]
        ]);
        start();
    });
});

asyncTest('append JData', function () {
    expect(1);

    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);
    d1.append(d2).get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',    'apple' ],
            [ 'car',        'screen',    'phone' ],
            [ 'sign',        'bagel',    'chips' ]
        ]);
        start();
    });
});

test('failed append (columns not the same)', function () {
    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_a', 'column_b', 'column_d' ],

        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset1);

    throws(function () {
        d.append(dataset2);
    });
});

test('failed append (different number of columns)', function () {
    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_a', 'column_b' ],

        [ 'gummy',       'power' ],
        [ 'car',        'screen' ],
        [ 'sign',        'bagel' ]
    ];

    var d = new JData(dataset1);

    throws(function () {
        d.append(dataset2);
    });
});

asyncTest('join (inner join on single field)', function () {
    expect(2);

    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_d', 'column_e', 'column_f' ],

        [ 'banana',      'power',    'apple' ],
        [ 'apple',      'screen',    'phone' ],
        [ 'cat',         'bagel',    'chips' ],
        [ 'cat',     'amsterdam',    'drops' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    var result_dataset;

    d1.join(d2, 'column_a', 'column_d');

    d1.sort('column_a', 'column_f').get_columns_and_records(function (columns, rows) {
        deepEqual(columns, {
            column_a: {
                agg_type  : 'max',
                sort_type : 'alpha',
                title     : 'column_a',
                index     : 0
            },
            column_b: {
                agg_type  : 'max',
                sort_type : 'alpha',
                title     : 'column_b',
                index     : 1
            },
            column_c: {
                agg_type  : 'max',
                sort_type : 'alpha',
                title     : 'column_c',
                index     : 2
            },
            column_d: {
                agg_type  : 'max',
                sort_type : 'alpha',
                title     : 'column_d',
                index     : 3
            },
            column_e: {
                agg_type  : 'max',
                sort_type : 'alpha',
                title     : 'column_e',
                index     : 4
            },
            column_f: {
                agg_type  : 'max',
                sort_type : 'alpha',
                title     : 'column_f',
                index     : 5
            }
        });
        deepEqual(rows, [
            [ 'apple', 'violin', 'music', 'apple',    'screen', 'phone' ],
            [ 'banana', 'piano', 'gum',  'banana',     'power', 'apple' ],
            [ 'cat',   'tissue', 'dog',     'cat',     'bagel', 'chips' ],
            [ 'cat',   'tissue', 'dog',     'cat', 'amsterdam', 'drops' ]
        ]);

        start();
    });
});

asyncTest('join (left outer join on single field)', function () {
    expect(1);

    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'dump',    'amsterdam',    'drops' ]
    ];
    var dataset2 = [
        [ 'column_d', 'column_e', 'column_f' ],

        [ 'banana',      'power',    'apple' ],
        [ 'apple',      'screen',    'phone' ],
        [ 'cat',         'bagel',    'chips' ],
        [ 'car',          'nuts',     'axes' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    d1.join(d2, 'column_a', 'column_d', 'left');

    d1.sort('column_a', 'column_f').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple',   'violin', 'music',  'apple',    'screen', 'phone' ],
            [ 'banana',   'piano',   'gum', 'banana',     'power', 'apple' ],
            [ 'cat',     'tissue',   'dog',    'cat',     'bagel', 'chips' ],
            [ 'dump', 'amsterdam', 'drops',       '',          '',      '' ],
        ]);
        start();
    });
});

asyncTest('join (right outer join on single field', function () {
    expect(1);

    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'dump',    'amsterdam',    'drops' ]
    ];
    var dataset2 = [
        [ 'column_d', 'column_e', 'column_f' ],

        [ 'banana',      'power',    'apple' ],
        [ 'apple',      'screen',    'phone' ],
        [ 'cat',         'bagel',    'chips' ],
        [ 'car',          'nuts',     'axes' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    d1.join(d2, 'column_a', 'column_d', 'right');

    d1.sort('column_a', 'column_f').get_dataset(function (result) {
        deepEqual(result, [
            [ '',              '',      '',    'car',      'nuts',  'axes' ],
            [ 'apple',   'violin', 'music',  'apple',    'screen', 'phone' ],
            [ 'banana',   'piano',   'gum', 'banana',     'power', 'apple' ],
            [ 'cat',     'tissue',   'dog',    'cat',     'bagel', 'chips' ]
        ]);
        start();
    });
});

asyncTest('join (inner join on multiple fields)', function () {
    expect(1);

    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_d', 'column_e', 'column_f' ],

        [ 'banana',      'power',    'apple' ],
        [ 'apple',      'screen',    'phone' ],
        [ 'cat',         'bagel',    'chips' ],
        [ 'cat',     'amsterdam',    'drops' ],
        [ 'cat',        'tissue',    'drops' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    d1.join(
        d2,
        [ 'column_a', 'column_b' ],
        [ 'column_d', 'column_e' ]
    );

    var result = d1.get_dataset(function (result) {
        deepEqual(result, [
            [ 'cat', 'tissue', 'dog', 'cat', 'tissue', 'drops' ]
        ]);
        start();
    });
});

test('failed join (unknown join type)', function () {
    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_d', 'column_e', 'column_f' ],

        [ 'banana',      'power',    'apple' ],
        [ 'apple',      'screen',    'phone' ],
        [ 'cat',         'bagel',    'chips' ],
        [ 'cat',     'amsterdam',    'drops' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    throws(function () {
        d1.join(d2, 'column_a', 'column_d', 'crazy');
    });
});

test('failed join (columns with same name)', function () {
    var dataset1 = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
    ];
    var dataset2 = [
        [ 'column_d', 'column_e', 'column_c' ],

        [ 'banana',      'power',    'apple' ],
        [ 'apple',      'screen',    'phone' ],
        [ 'cat',         'bagel',    'chips' ],
        [ 'cat',     'amsterdam',    'drops' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    throws(function () {
        d1.join(d2, 'column_a', 'column_d');
    });
});

asyncTest('prepend column names', function () {
    expect(1);

    var dataset = [
        [
            { name: 'column_a', sort_type: 'alpha', agg_type: 'max' },
            { name: 'column_b', sort_type: 'alpha', agg_type: 'max' },
            { name: 'column_c', sort_type: 'alpha', agg_type: 'min' }
        ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).prepend_column_names('p_').get_columns(function (columns) {
        deepEqual(columns, {
            p_column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            p_column_b: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_b',
                index     : 1
            },
            p_column_c: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });

        start();
    });
});

asyncTest('alter column name', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).alter_column_name('column_a', 'a_column');

    d.get_columns(function (columns) {
        deepEqual(columns, {
            'a_column': {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            'column_b': {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_b',
                index     : 1
            },
            'column_c': {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_c',
                index     : 2
            }
        });
        start();
    });
});

test('alter column name (fails if changing to already existing column name)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    throws(function () {
        d.alter_column_name('column_a', 'column_b');
    });
});

asyncTest('alter column sort type', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).alter_column_sort_type('column_a', 'random');

    d.get_columns(function (columns) {
        equal(columns['column_a']['sort_type'], 'random');
        start();
    });
});

asyncTest('alter column aggregate type', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).alter_column_aggregate_type('column_a', 'random');

    d.get_columns(function (columns) {
        equal(columns['column_a']['agg_type'], 'random'); 
        start();
    });
});

asyncTest('alter column title', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).alter_column_title('column_a', 'random');

    d.get_columns(function (columns) {
        equal(columns['column_a']['title'], 'random'); 
        start();
    });
});

asyncTest('group (single field sum)', function () {
    expect(1);

    var dataset = [
        [
            { name : 'column_a', sort_type : 'alpha', agg_type : 'max' },
            { name : 'column_b', sort_type : 'num',   agg_type : 'sum' },
        ],

        [ 'apple',  453 ],
        [ 'cat',    663 ],
        [ 'apple',  123 ],
        [ 'gummy',  34  ]
    ];

    var d = new JData(dataset).group('column_a').sort('column_a');

    d.get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 576 ],
            [ 'cat',   663 ],
            [ 'gummy',  34 ]
        ]);
        start();
    });

});

asyncTest('group (single field max)', function () {
    expect(1);

    var dataset = [
        [
            { name : 'column_a', sort_type : 'alpha', agg_type : 'max' },
            { name : 'column_b', sort_type : 'num',   agg_type : 'max' },
        ],

        [ 'apple',  453 ],
        [ 'cat',    663 ],
        [ 'apple',  123 ],
        [ 'gummy',  34  ]
    ];

    var d = new JData(dataset).group('column_a').sort('column_a');

    d.get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 453 ],
            [ 'cat',   663 ],
            [ 'gummy',  34 ]
        ]);
        start();
    });

});

asyncTest('group (single field min)', function () {
    expect(1);

    var dataset = [
        [
            { name : 'column_a', sort_type : 'alpha', agg_type : 'max' },
            { name : 'column_b', sort_type : 'num',   agg_type : 'min' },
        ],

        [ 'apple',  453 ],
        [ 'cat',    663 ],
        [ 'apple',  123 ],
        [ 'gummy',  34  ]
    ];

    var d = new JData(dataset).group('column_a').sort('column_a');

    d.get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 123 ],
            [ 'cat',   663 ],
            [ 'gummy',  34 ]
        ]);
        start();
    });
});

asyncTest('group (multi-field)', function () {
    expect(1);

    var dataset = [
        [
            { name : 'column_a', sort_type : 'alpha', agg_type : 'max' },
            { name : 'column_b', sort_type : 'alpha', agg_type : 'max' },
            { name : 'column_c', sort_type : 'num',   agg_type : 'sum' },
        ],

        [ 'apple',  'violin', 453 ],
        [ 'cat',    'tissue', 663 ],
        [ 'apple',  'piano',  123 ],
        [ 'gummy',  'power',  34  ],
        [ 'apple',  'piano',  95  ],
        [ 'gummy',  'power',  768 ]
    ];

    var d = new JData(dataset).group('column_a', 'column_b').sort('column_a', 'column_b');

    d.get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple',  'piano',  218 ],
            [ 'apple',  'violin', 453 ],
            [ 'cat',    'tissue', 663 ],
            [ 'gummy',   'power', 802 ]
        ]);
        start();
    });
});

asyncTest('get partitioned (single field)', function () {
    expect(9);

    var dataset = [
        [
            { name: 'column_a', sort_type: 'alpha', agg_type: 'max' },
            { name: 'column_b', sort_type: 'alpha', agg_type: 'min' },
            { name: 'column_c', sort_type: 'alpha', agg_type: 'min' },
        ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ],
        [ 'apple',      'trance',   'camaro' ],
        [ 'cat',           'soy',  'blender' ],
        [ 'banana',   'eyedrops',      'tie' ],
        [ 'apple',        'body',      'key' ]
    ];

    var d = new JData(dataset).partition('column_a');

    var partition_keys,
        apple_partition  = {},
        banana_partition = {},
        cat_partition    = {},
        gummy_partition  = {};

    d.render(function () {
        deepEqual(partition_keys.sort(), [
            [ 'apple'  ],
            [ 'banana' ],
            [ 'cat'    ],
            [ 'gummy'  ]
        ]);

        deepEqual(apple_partition['columns'], {
            column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(apple_partition['rows'], [
            [ 'apple',   'body',    'key' ],
            [ 'apple', 'trance', 'camaro' ],
            [ 'apple', 'violin',  'music' ]
        ]);

        deepEqual(banana_partition['columns'], {
            column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(banana_partition['rows'], [
            [ 'banana', 'eyedrops', 'tie' ],
            [ 'banana',    'piano', 'gum' ]
        ]);

        deepEqual(cat_partition['columns'], {
            column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(cat_partition['rows'], [
            [ 'cat',    'soy', 'blender' ],
            [ 'cat', 'tissue',     'dog' ]
        ]);

        deepEqual(gummy_partition['columns'], {
            column_a: {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c: {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(gummy_partition['rows'], [
            [ 'gummy', 'power', 'star' ]
        ]);

        start();
    });

    d.get_partition_keys(function (keys) {
        partition_keys = keys;
    });

    d.get_partitioned(function (partition) {
        partition.sort('column_b').get_columns_and_records(function (columns, rows) {
            apple_partition['columns'] = columns;
            apple_partition['rows']    = rows;
        });
    }, 'apple');

    d.get_partitioned(function (partition) {
        partition.sort('column_b').get_columns_and_records(function (columns, rows) {
            banana_partition['columns'] = columns;
            banana_partition['rows']    = rows;
        });
    }, 'banana');

    d.get_partitioned(function (partition) {
        partition.sort('column_b').get_columns_and_records(function (columns, rows) {
            cat_partition['columns'] = columns;
            cat_partition['rows']    = rows;
        });
    }, 'cat');

    d.get_partitioned(function (partition) {
        partition.sort('column_b').get_columns_and_records(function (columns, rows) {
            gummy_partition['columns'] = columns;
            gummy_partition['rows']    = rows;
        });
    }, 'gummy');

    d.render();
});

asyncTest('get partitioned (multi-field)', function () {
    expect(13);

    var dataset = [
        [
            { name: 'column_a', sort_type: 'alpha', agg_type: 'max' },
            { name: 'column_b', sort_type: 'alpha', agg_type: 'min' },
            { name: 'column_c', sort_type: 'alpha', agg_type: 'min' },
        ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ],
        [ 'apple',      'trance',   'camaro' ],
        [ 'cat',           'soy',  'blender' ],
        [ 'banana',      'piano',      'tie' ],
        [ 'apple',      'violin',      'key' ]
    ];

    var d = new JData(dataset).partition('column_a', 'column_b');

    var partition_keys,
        apple_trance_partition = {},
        apple_violin_partition = {},
        banana_piano_partition = {},
        cat_soy_partition      = {},
        cat_tissue_partition   = {},
        gummy_power_partition  = {};

    d.render(function () {
        deepEqual(partition_keys.sort(), [
            [ 'apple', 'trance' ],
            [ 'apple', 'violin' ],
            [ 'banana', 'piano' ],
            [ 'cat',      'soy' ],
            [ 'cat',   'tissue' ],
            [ 'gummy', 'power'  ]
        ]);

        deepEqual(apple_trance_partition['columns'], {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(apple_trance_partition['rows'], [
            [ 'apple', 'trance', 'camaro' ]
        ]);

        deepEqual(apple_violin_partition['columns'], {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(apple_violin_partition['rows'], [
            [ 'apple', 'violin',   'key' ],
            [ 'apple', 'violin', 'music' ]
        ]);

        deepEqual(banana_piano_partition['columns'], {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(banana_piano_partition['rows'], [
            [ 'banana', 'piano', 'gum' ],
            [ 'banana', 'piano', 'tie' ]
        ]);

        deepEqual(cat_soy_partition['columns'], {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(cat_soy_partition['rows'], [
            [ 'cat', 'soy',  'blender' ]
        ]);

        deepEqual(cat_tissue_partition['columns'], {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(cat_tissue_partition['rows'], [
            [ 'cat', 'tissue', 'dog' ]
        ]);

        deepEqual(gummy_power_partition['columns'], {
            column_a : {
                sort_type : 'alpha',
                agg_type  : 'max',
                title     : 'column_a',
                index     : 0
            },
            column_b : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_b',
                index     : 1
            },
            column_c : {
                sort_type : 'alpha',
                agg_type  : 'min',
                title     : 'column_c',
                index     : 2
            }
        });
        deepEqual(gummy_power_partition['rows'], [
            [ 'gummy', 'power', 'star' ]
        ]);

        start();
    });

    d.get_partition_keys(function (keys) {
        partition_keys = keys;
    });

    d.get_partitioned(function (partition) {
        partition.sort('column_c').get_columns_and_records(function (columns, rows) {
            apple_trance_partition['columns'] = columns;
            apple_trance_partition['rows']    = rows;
        });
    }, 'apple', 'trance');

    d.get_partitioned(function (partition) {
        partition.sort('column_c').get_columns_and_records(function (columns, rows) {
            apple_violin_partition['columns'] = columns;
            apple_violin_partition['rows']    = rows;
        });
    }, 'apple', 'violin');

    d.get_partitioned(function (partition) {
        partition.sort('column_c').get_columns_and_records(function (columns, rows) {
            banana_piano_partition['columns'] = columns;
            banana_piano_partition['rows']    = rows;
        });
    }, 'banana', 'piano');

    d.get_partitioned(function (partition) {
        partition.sort('column_c').get_columns_and_records(function (columns, rows) {
            cat_soy_partition['columns'] = columns;
            cat_soy_partition['rows']    = rows;
        });
    }, 'cat', 'soy');

    d.get_partitioned(function (partition) {
        partition.sort('column_c').get_columns_and_records(function (columns, rows) {
            cat_tissue_partition['columns'] = columns;
            cat_tissue_partition['rows']    = rows;
        });
    }, 'cat', 'tissue');

    d.get_partitioned(function (partition) {
        partition.sort('column_c').get_columns_and_records(function (columns, rows) {
            gummy_power_partition['columns'] = columns;
            gummy_power_partition['rows']    = rows;
        });
    }, 'gummy', 'power');

    d.render();
});
