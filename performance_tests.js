JSLitmus.test('join', function () {
    var d1 = new JData(AoA_Dataset);
    var d2 = new JData(AoA_Dataset).prepend_column_names('p_');

    d1.join(d2, 'column_b', 'p_column_b');
});
