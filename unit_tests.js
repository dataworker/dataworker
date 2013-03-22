function assert_arrays_eq(array1, array2) {
    ok(array1, 'array1 does not exist');
    ok(array2, 'array2 does not exist');

    equal(array1.length, array2.length, 'arrays are different lengths');
    
    for (var i = 0; i < array1.length; i++) {
        deepEqual(array1[i], array2[i], 'arrays not equal at index ' + i);
    }
}

test('construct', function () {
    var d = new JData(AoA_Dataset);
    ok(d instanceof JData);
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

    equal(result.length, 1, 'expected: 1 row');
    assert_arrays_eq(result[0], [ 'apple', 'violin', 'music' ]);
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

    equal(result.length, 1, 'expected: 1 row');
    assert_arrays_eq(result[0], [ 'apple', 'violin', 'music' ]);

    d = new JData(dataset);
    result = d.filter(/apple/, 'column_b').get_dataset();

    equal(result.length, 0, 'expected: 0 rows');
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

    equal(result.length, 2, 'expected: 2 rows');
    assert_arrays_eq(result[0], [ 'apple', 'violin', 'music' ]);
    assert_arrays_eq(result[1], [ 'gummy', 'power', 'apple' ]);

    d = new JData(dataset);
    result = d.filter(/piano/, 'column_a', 'column_c').get_dataset();

    equal(result.length, 0, 'expected: 0 rows');
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

    equal(result.length, 1, 'expected: 1 row');
    assert_arrays_eq(result[0], [ 'apple', 'violin', 'music' ]);

    d = new JData(dataset);
    result = d.limit(2).get_dataset();

    equal(result.length, 2, 'expected: 2 rows');
    assert_arrays_eq(result[0], [ 'apple', 'violin', 'music' ]);
    assert_arrays_eq(result[1], [ 'cat', 'tissue', 'dog' ]);
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

    equal(result.length, 4, 'expected: 4 rows');
    assert_arrays_eq(result[0], [ 'apple' ]);
    assert_arrays_eq(result[1], [ 'cat' ]);
    assert_arrays_eq(result[2], [ 'banana' ]);
    assert_arrays_eq(result[3], [ 'gummy' ]);
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

    equal(result.length, 4, 'expected: 4 rows');
    assert_arrays_eq(result[0], [ 'banana',      'piano',      'gum' ]);
    assert_arrays_eq(result[1], [ 'gummy',       'power',    'apple' ]);
    assert_arrays_eq(result[2], [ 'cat',        'tissue',      'dog' ]);
    assert_arrays_eq(result[3], [ 'apple',      'violin',    'music' ]);
});

test('num sort', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',        5 ],
        [ 'cat',        'tissue',       85 ],
        [ 'banana',      'piano',       45 ],
        [ 'gummy',       'power',       82 ]
    ];

    var d = new JData(dataset);
    var result = d.sort([ { column: 'column_c', sort_type: 'num' } ]).get_dataset();

    equal(result.length, 4, 'expected: 4 rows');
    assert_arrays_eq(result[0], [ 'apple',      'violin',        5 ]);
    assert_arrays_eq(result[1], [ 'banana',      'piano',       45 ]);
    assert_arrays_eq(result[2], [ 'gummy',       'power',       82 ]);
    assert_arrays_eq(result[3], [ 'cat',        'tissue',       85 ]);
});

test('multi-column sort', function () {
    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',        5 ],
        [ 'cat',        'tissue',       85 ],
        [ 'banana',      'piano',       45 ],
        [ 'cat',         'power',       98 ]
    ];

    var d = new JData(dataset);
    var result = d.sort([
        { column: 'column_a', sort_type: 'alpha' },
        { column: 'column_c', sort_type: 'num' }
    ]).get_dataset();

    equal(result.length, 4, 'expected: 4 rows');
    assert_arrays_eq(result[0], [ 'apple',      'violin',        5 ]);
    assert_arrays_eq(result[1], [ 'banana',      'piano',       45 ]);
    assert_arrays_eq(result[2], [ 'cat',        'tissue',       85 ]);
    assert_arrays_eq(result[3], [ 'cat',         'power',       98 ]);
});
