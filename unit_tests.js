test('construct (simple columns)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    ok(d instanceof JData);
    deepEqual(d.get_columns(), [ 'column_a', 'column_b', 'column_c' ]);
    deepEqual(d._columns_idx_xref, {
        column_a: 0,
        column_b: 1,
        column_c: 2
    });
    deepEqual(d._columns_metadata, {
        column_a: {
            agg_type: 'max',
            sort_type: 'alpha'
        },
        column_b: {
            agg_type: 'max',
            sort_type: 'alpha'
        },
        column_c: {
            agg_type: 'max',
            sort_type: 'alpha'
        }
    });
});

test('construct (complex columns)', function () {
    var dataset = [
        [
            {
                name: 'column_a',
                agg_type: 'max',
                sort_type: 'alpha'
            },
            {
                name: 'column_b',
                agg_type: 'max',
                sort_type: 'alpha'
            },
            {
                name: 'column_c',
                agg_type: 'min',
                sort_type: 'alpha'
            }
        ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    ok(d instanceof JData);
    deepEqual(d.get_columns(), [ 'column_a', 'column_b', 'column_c' ]);
    deepEqual(d._columns_idx_xref, {
        column_a: 0,
        column_b: 1,
        column_c: 2
    });
    deepEqual(d._columns_metadata, {
        column_a: {
            agg_type: 'max',
            sort_type: 'alpha'
        },
        column_b: {
            agg_type: 'max',
            sort_type: 'alpha'
        },
        column_c: {
            agg_type: 'min',
            sort_type: 'alpha'
        }
    });
});

test('filter', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.filter(/apple/);

    var result = d.get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music' ],
    ]);
});

test('column-restricted filter (single column)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);
    var result = d.filter(/apple/, 'column_a').get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music' ],
    ]);

    d = new JData(dataset);
    result = d.filter(/apple/, 'column_b').get_dataset();

    deepEqual(result, []);
});

test('column-restricted filter (multi-column)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    var result = d.filter(/apple/, 'column_a', 'column_c')
                  .sort('column_a')
                  .get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music' ],
        [ 'gummy', 'power', 'apple'  ]
    ]);

    d = new JData(dataset);
    result = d.filter(/piano/, 'column_a', 'column_c').get_dataset();

    deepEqual(result, []);
});

test('limit', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    var result = d.limit(1).get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music' ]
    ]);

    d = new JData(dataset);
    result = d.limit(2).get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music' ],
        [ 'cat', 'tissue', 'dog'     ]
    ]);
});

test('remove columns', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    var result = d.remove_columns('column_b', 'column_c').get_dataset();

    deepEqual(result, [
        [ 'apple'  ],
        [ 'cat'    ],
        [ 'banana' ],
        [ 'gummy'  ]
    ]);
});

test('sort (alpha)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    var result = d.sort('column_b').get_dataset();

    deepEqual(result, [
        [ 'banana', 'piano',  'gum'   ],
        [ 'gummy',  'power',  'apple' ],
        [ 'cat',    'tissue', 'dog'   ],
        [ 'apple',  'violin', 'music' ]
    ]);
});

test('sort (reverse alpha)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    var result = d.sort('-column_b').get_dataset();

    deepEqual(result, [
        [ 'apple',  'violin', 'music' ],
        [ 'cat',    'tissue', 'dog'   ],
        [ 'gummy',  'power',  'apple' ],
        [ 'banana', 'piano',  'gum'   ]
    ]);
});

test('sort (num)', function () {
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
        [ 'gummy',       'power',         82 ]
    ];

    var d = new JData(dataset);
    var result = d.sort('column_c').get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin',  5 ],
        [ 'banana', 'piano', 45 ],
        [ 'gummy',  'power', 82 ],
        [ 'cat',   'tissue', 85 ]
    ]);
});

test('sort (multi-column)', function () {
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
    var result = d.sort('column_a', 'column_c').get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin',  5 ],
        [ 'banana', 'piano', 45 ],
        [ 'cat',   'tissue', 85 ],
        [ 'cat',    'power', 98 ]
    ]);
});

test('paginate (set page)', function () {
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

    d.set_page(3);
    equal(d._current_page, 2);

    d.set_page(1);
    equal(d._current_page, 0);

    d.set_page(0);
    equal(d._current_page, 0);

    d.set_page(-1);
    equal(d._current_page, 0);
});

test('paginate (next page)', function () {
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

    var page1 = d.get_next_page();
    var page2 = d.get_next_page();
    var page3 = d.get_next_page();
    var page4 = d.get_next_page();

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
});

test('paginate (previous page)', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ];

    var d = new JData(dataset).paginate(2).set_page(4);

    var page3 = d.get_previous_page();
    var page2 = d.get_previous_page();
    var page1 = d.get_previous_page();
    var page0 = d.get_previous_page();

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
});

test('paginate (get current page)', function () {
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
    var page = d.get_page();

    deepEqual(page, [
        [ 'banana', 'piano',   'gum' ],
        [ 'gummy',  'power', 'apple' ],
    ]);
});

test('paginate (get specific page)', function () {
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
    var page = d.get_page(2);

    deepEqual(page, [
        [ 'banana', 'piano',   'gum' ],
        [ 'gummy',  'power', 'apple' ],
    ]);
});

test('append', function () {
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
    var result = d.append(dataset2).get_dataset();

    deepEqual(result, [
        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ]);
});

test('append JData', function () {
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
    var result = d1.append(d2).get_dataset();

    deepEqual(result, [
        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ],
        [ 'car',        'screen',    'phone' ],
        [ 'sign',        'bagel',    'chips' ]
    ]);
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

test('join (inner join on single field)', function () {
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

    d1.join(d2, 'column_a', 'column_d');

    var result = d1.sort('column_a', 'column_f').get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music', 'apple',    'screen', 'phone' ],
        [ 'banana', 'piano', 'gum',  'banana',     'power', 'apple' ],
        [ 'cat',   'tissue', 'dog',     'cat',     'bagel', 'chips' ],
        [ 'cat',   'tissue', 'dog',     'cat', 'amsterdam', 'drops' ]
    ]);

    deepEqual(
        d1._columns,
        [ 'column_a', 'column_b', 'column_c', 'column_d', 'column_e', 'column_f' ]
    );

    deepEqual(d1._columns_metadata, {
        column_a: {
            agg_type: 'max',
            sort_type: 'alpha',
        },
        column_b: {
            agg_type: 'max',
            sort_type: 'alpha',
        },
        column_c: {
            agg_type: 'max',
            sort_type: 'alpha',
        },
        column_d: {
            agg_type: 'max',
            sort_type: 'alpha',
        },
        column_e: {
            agg_type: 'max',
            sort_type: 'alpha',
        },
        column_f: {
            agg_type: 'max',
            sort_type: 'alpha',
        }
    });
});

test('join (left outer join on single field)', function () {
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

    var result = d1.sort('column_a', 'column_f').get_dataset();

    deepEqual(result, [
        [ 'apple',   'violin', 'music',  'apple',    'screen', 'phone' ],
        [ 'banana',   'piano',   'gum', 'banana',     'power', 'apple' ],
        [ 'cat',     'tissue',   'dog',    'cat',     'bagel', 'chips' ],
        [ 'dump', 'amsterdam', 'drops',       '',          '',      '' ],
    ]);
});

test('join (right outer join on single field', function () {
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

    var result = d1.sort('column_a', 'column_f').get_dataset();

    deepEqual(result, [
        [ '',              '',      '',    'car',      'nuts',  'axes' ],
        [ 'apple',   'violin', 'music',  'apple',    'screen', 'phone' ],
        [ 'banana',   'piano',   'gum', 'banana',     'power', 'apple' ],
        [ 'cat',     'tissue',   'dog',    'cat',     'bagel', 'chips' ]
    ]);
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

test('prepend column names', function () {
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

    var d = new JData(dataset).prepend_column_names('p_');

    deepEqual(d._columns, [ 'p_column_a', 'p_column_b', 'p_column_c' ]);
    deepEqual(d._columns_idx_xref, {
        p_column_a: 0,
        p_column_b: 1,
        p_column_c: 2
    });
    deepEqual(d._columns_metadata, {
        p_column_a: {
            sort_type: 'alpha',
            agg_type: 'max'
        },
        p_column_b: {
            sort_type: 'alpha',
            agg_type: 'max'
        },
        p_column_c: {
            sort_type: 'alpha',
            agg_type: 'min'
        },
    });
});

test('group (single field)', function () {
});

test('group (multi-field)', function () {
});
