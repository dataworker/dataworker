JSLitmus.test('sort', function () {
    var d1 = new JData(AoA_Dataset);

    d1.sort('-column_b', '-column_d');
});
