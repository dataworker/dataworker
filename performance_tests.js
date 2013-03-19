var d = new JData(AoA_Dataset);

JSLitmus.test('construct', function () {
    d = new JData(AoA_Dataset);
});

JSLitmus.test('alpha sort', function () {
    d = new JData(AoA_Dataset);
    d.sort([ { column: 'column_a', sort_type: 'alpha' } ]);
});

JSLitmus.test('num sort', function () {
    d = new JData(AoA_Dataset);
    d.sort([ { column: 'column_b', sort_type: 'num' } ]);
});

JSLitmus.test('alpha sort 2', function () {
    d = new JData(AoA_Dataset);
    d.sort([
        { column: 'column_a', sort_type: 'alpha' },
        { column: 'column_c', sort_type: 'alpha' },
    ]);
});

JSLitmus.test('num sort 2', function () {
    d = new JData(AoA_Dataset);
    d.sort([
        { column: 'column_b', sort_type: 'num' },
        { column: 'column_d', sort_type: 'num' },
    ]);
});

JSLitmus.test('filter dataset columns', function () {
    d = new JData(AoA_Dataset);
    d.get_dataset([ 'column_a', 'column_c' ]);
});
