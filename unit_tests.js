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
});

test('construct (complex columns)', function () {
    var dataset = [
        [
            {
                name: 'column_a',
                agg: 'max'
            },
            {
                name: 'column_b',
                agg: 'max'
            },
            {
                name: 'column_c',
                agg: 'max'
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
                  .sort([ { column: 'column_a', sort_type: 'alpha' } ])
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

test('alpha sort', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    var result = d.sort([ { column: 'column_b', sort_type: 'alpha' } ]).get_dataset();

    deepEqual(result, [
        [ 'banana', 'piano',  'gum'   ],
        [ 'gummy',  'power',  'apple' ],
        [ 'cat',    'tissue', 'dog'   ],
        [ 'apple',  'violin', 'music' ]
    ]);
});

test('num sort', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',          5 ],
        [ 'cat',        'tissue',         85 ],
        [ 'banana',      'piano',         45 ],
        [ 'gummy',       'power',         82 ]
    ];

    var d = new JData(dataset);
    var result = d.sort([ { column: 'column_c', sort_type: 'num' } ]).get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin',  5 ],
        [ 'banana', 'piano', 45 ],
        [ 'gummy',  'power', 82 ],
        [ 'cat',   'tissue', 85 ]
    ]);
});

test('multi-column sort', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',          5 ],
        [ 'cat',        'tissue',         85 ],
        [ 'banana',      'piano',         45 ],
        [ 'cat',         'power',         98 ]
    ];

    var d = new JData(dataset);
    var result = d.sort([
        { column: 'column_a', sort_type: 'alpha' },
        { column: 'column_c', sort_type: 'num' }
    ]).get_dataset();

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

test('join', function () {
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

    var d = new JData(dataset1).join(dataset2, 'column_a', 'column_d');
    var result = d.sort([
        { column: "column_a", sort_type: "alpha" },
        { column: "column_f", sort_type: "alpha" },
    ]).get_dataset();

    deepEqual(result, [
        [ 'apple', 'violin', 'music', 'apple',    'screen', 'phone' ],
        [ 'banana', 'piano', 'gum',  'banana',     'power', 'apple' ],
        [ 'cat',   'tissue', 'dog',     'cat',     'bagel', 'chips' ],
        [ 'cat',   'tissue', 'dog',     'cat', 'amsterdam', 'drops' ]
    ]);
});
