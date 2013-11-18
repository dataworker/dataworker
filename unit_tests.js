/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 *                                                                           *
 * Tests for ActionQueue                                                     *
 *                                                                           *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

module('ActionQueue');

test('construct', function () {
    var q = new ActionQueue();

    ok(q instanceof ActionQueue);
});

test('queue action (not in action)', function () {
    var q = new ActionQueue(), actionDone = false;

    q.queueNext(function () {
        actionDone = true;
        q.finishAction();
    });

    ok(actionDone);
});

test('queue action (in action)', function () {
    var q = new ActionQueue();

    function toQ() { q.finishAction(); };

    q._isInAction = true;
    q.queueNext(toQ);

    deepEqual(
        q._queue,
        [
            [ toQ ]
        ]
    );
});

test('queue action (named function with args)', function () {
    var q = new ActionQueue(), toSet = 'arghh';

    function toQ(arg) {
        toSet = arg;
        q.finishAction();
    };

    q.queueNext(toQ, 'Hello, world!');

    equal(toSet, 'Hello, world!');
});

test('queue action (within queued action)', function () {
    expect(7);

    var q = new ActionQueue(), steps = 0;

    q._isInAction = true;

    q.queueNext(function () {
        equal(steps++, 0);

        this.queueNext(function () {
            equal(steps++, 1);

            this.queueNext(function () {
                equal(steps++, 2);
                this.finishAction();
            });

            this.finishAction();
        });

        this.finishAction();
    }).queueNext(function () {
        equal(steps++, 3);

        this.queueNext(function () {
            equal(steps++, 4);

            this.finishAction();
        });

        this.finishAction();
    }).queueNext(function () {
        equal(steps++, 5);

        this.finishAction();
    });

    q.finishAction();

    equal(steps, 6);
});

test('finish action', function () {
    var q = new ActionQueue(),
        action1Done = false, action2Done = false;

    function toQ1() {
        action1Done = true;
        q.finishAction();
    };
    function toQ2() {
        action2Done = true;
        q.finishAction();
    };

    q._isInAction = true;

    q.queueNext(toQ1).queueNext(toQ2).finishAction();

    ok(action1Done);
    ok(action2Done);
});

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 *                                                                           *
 * Tests for JData                                                           *
 *                                                                           *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

module('JData');

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
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            },
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 1
            },
            column_c: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_c',
                name           : 'column_c',
                is_visible     : true,
                index          : 2
            }
        });

        start();
    }).finish();
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
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'Column A',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            },
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'Column B',
                name           : 'column_b',
                is_visible     : true,
                index          : 1
            },
            column_c: {
                sort_type      : 'alpha',
                agg_type       : 'min',
                title          : 'Column C',
                name           : 'column_c',
                is_visible     : true,
                index          : 2
            }
        });

        start();
    }).finish();
});

asyncTest('apply filter (simple, unrestricted)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.apply_filter(/apple/);

    d.get_dataset(function(result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
        ]);
        start();
    }).finish();
});

asyncTest('apply filter (simple, column-restricted, single column, found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);
    d.apply_filter(/apple/, 'column_a').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
        ]);
        start();
    }).finish();
});

asyncTest('apply filter (simple, column-restricted, single column, not found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);
    d.apply_filter(/apple/, 'column_b').get_dataset(function (result) {
        deepEqual(result, []);
        start();
    }).finish();
});


asyncTest('apply filter (simple, column-restricted, multi-column, found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.apply_filter(/apple/, 'column_a', 'column_c')
     .sort('column_a')
     .get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
            [ 'gummy', 'power', 'apple'  ]
        ]);
        start();
     }).finish();
});

asyncTest('apply filter (simple, column-restricted, multi-column, not found)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.apply_filter(/piano/, 'column_a', 'column_c').get_dataset(function (result) {
        deepEqual(result, []);
        start();
    }).finish();
});

asyncTest('apply filter (complex, single)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.apply_filter(
        {
            column : 'column_a',
            regex  : '^apple$'
        }
    ).get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
        ]);
        start();
    }).finish();
});

asyncTest('apply filter (complex, multi)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'cat',         'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.apply_filter(
        {
            column : 'column_a',
            regex  : '^cat$'
        },
        {
            column : 'column_c',
            regex  : '^dog|gum$'
        }
    ).get_dataset(function (result) {
        deepEqual(result, [
            [ 'cat',        'tissue',      'dog' ],
            [ 'cat',         'piano',      'gum' ],
        ]);
        start();
    }).finish();
});

asyncTest('clear filter', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.apply_filter(/apple/).clear_filters();

    d.get_dataset(function(result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',     'star' ]
        ]);
        start();
    }).finish();
});

asyncTest('filter (hard)', function () {
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
    }).finish();
});

asyncTest('limit', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',    'apple' ]
    ];

    var d = new JData(dataset);
    d.apply_limit(2).get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
            [ 'cat',   'tissue', 'dog'   ]
        ]);
        start();
    }).finish();
});

asyncTest('limit (hard)', function () {
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
        start();
    }).finish();
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
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            }
        });
        deepEqual(rows, [
            [ 'apple'  ],
            [ 'cat'    ],
            [ 'banana' ],
            [ 'gummy'  ]
        ]);

        start();
    }).finish();
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
    }).finish();
});

// TODO: localeCompare doesn't seem to work properly in Web Worker threads.
//asyncTest('sort (locale-considering alpha)', function () {
//    expect(1);

//    var dataset = [
//        [
//            {
//                name: 'column_a',
//                agg_type: 'max',
//                sort_type: 'locale_alpha',
//                title: 'Column A'
//            },
//            {
//                name: 'column_b',
//                agg_type: 'max',
//                sort_type: 'alpha',
//                title: 'Column B'
//            },
//            {
//                name: 'column_c',
//                agg_type: 'min',
//                sort_type: 'alpha',
//                title: 'Column C'
//            }
//        ],

//        [ 'rip',        'violin',    'music' ],
//        [ 'résumé',     'tissue',      'dog' ],
//        [ 'gummy',       'power',     'star' ],
//        [ 'banana',      'piano',      'gum' ]
//    ];

//    var d = new JData(dataset);

//    d.sort('column_a').get_dataset(function (result) {
//        deepEqual(result, [
//            [ 'banana',      'piano',      'gum' ],
//            [ 'gummy',       'power',     'star' ],
//            [ 'résumé',     'tissue',      'dog' ],
//            [ 'rip',        'violin',    'music' ]
//        ]);

//        start();
//    }).finish();
//});

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
    }).finish();
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
    }).finish();
});

asyncTest('sort (num w/ decimals)', function () {
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

        [ 'apple',      'violin',          '8.0' ],
        [ 'cat',        'tissue',         '85.0' ],
        [ 'banana',      'piano',         '45.0' ],
        [ 'gummy',       'power',         '82.0' ]
    ];

    var d = new JData(dataset);
    d.sort('column_c').get_dataset(function (result) {
        deepEqual(result, [
            [ 'apple', 'violin',  '8.0' ],
            [ 'banana', 'piano', '45.0' ],
            [ 'gummy',  'power', '82.0' ],
            [ 'cat',   'tissue', '85.0' ]
        ]);
        start();
    }).finish();
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
    }).finish();
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
    }).finish();
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

    d.set_page(3).get_page(function (result) {
        deepEqual(result, [
            [ 'car',        'screen',    'phone' ],
            [ 'sign',        'bagel',    'chips' ]
        ]);
        start();
    }).finish();
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

    d.set_page(1).get_page(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ]
        ]);
        start();
    }).finish();
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

    d.set_page(0).get_page(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ]
        ]);
        start();
    }).finish();
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

    d.set_page(-1).get_page(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ]
        ]);
        start();
    }).finish();
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
    });

    var wait = function () {
        if (
            typeof(page1) !== "undefined" && page1.length > 0
            && typeof(page2) !== "undefined" && page2.length > 0
            && typeof(page3) !== "undefined" && page3.length > 0
            && typeof(page4) !== "undefined"
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
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
    })

    var wait = function () {
        if (
            typeof(page0) !== "undefined" && page0.length > 0
            && typeof(page1) !== "undefined" && page1.length > 0
            && typeof(page2) !== "undefined" && page2.length > 0
            && typeof(page3) !== "undefined" && page3.length > 0
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
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
    }).finish();
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
    }, 2).finish();
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
    }).finish();
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
    }).finish();
});

asyncTest('failed append (columns not the same)', function () {
    expect(1);

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

    var d = new JData(dataset1).on_error(function (error) {
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

asyncTest('failed append (different number of columns)', function () {
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

    var d = new JData(dataset1).on_error(function (error) {
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

    d1.join(d2, 'column_a', 'column_d');

    d1.sort('column_a', 'column_f').get_columns_and_records(function (columns, rows) {
        deepEqual(columns, {
            column_a: {
                agg_type       : 'max',
                sort_type      : 'alpha',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            },
            column_b: {
                agg_type       : 'max',
                sort_type      : 'alpha',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 1
            },
            column_c: {
                agg_type       : 'max',
                sort_type      : 'alpha',
                title          : 'column_c',
                name           : 'column_c',
                is_visible     : true,
                index          : 2
            },
            column_d: {
                agg_type       : 'max',
                sort_type      : 'alpha',
                title          : 'column_d',
                name           : 'column_d',
                is_visible     : true,
                index          : 3
            },
            column_e: {
                agg_type       : 'max',
                sort_type      : 'alpha',
                title          : 'column_e',
                name           : 'column_e',
                is_visible     : true,
                index          : 4
            },
            column_f: {
                agg_type       : 'max',
                sort_type      : 'alpha',
                title          : 'column_f',
                name           : 'column_f',
                is_visible     : true,
                index          : 5
            }
        });
        deepEqual(rows, [
            [ 'apple', 'violin', 'music', 'apple',    'screen', 'phone' ],
            [ 'banana', 'piano', 'gum',  'banana',     'power', 'apple' ],
            [ 'cat',   'tissue', 'dog',     'cat',     'bagel', 'chips' ],
            [ 'cat',   'tissue', 'dog',     'cat', 'amsterdam', 'drops' ]
        ]);

        start();
    }).finish();
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
    }).finish();
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
    }).finish();
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

    d1.get_dataset(function (result) {
        deepEqual(result, [
            [ 'cat', 'tissue', 'dog', 'cat', 'tissue', 'drops' ]
        ]);
        start();
    }).finish();
});

asyncTest('failed join (unknown join type)', function () {
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
        [ 'cat',     'amsterdam',    'drops' ]
    ];

    var d1 = new JData(dataset1);
    var d2 = new JData(dataset2);

    d1.on_error(function (error) {
        equal(error, 'Unknown join type.');

        start();
    });

    d1.join(d2, 'column_a', 'column_d', 'crazy').finish();
});

asyncTest('failed join (columns with same name)', function () {
    expect(1);

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

    d1.on_error(function (error) {
        equal(error, 'Column names overlap.');

        start();
    });

    d1.join(d2, 'column_a', 'column_d').finish();
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
                sort_type  : 'alpha',
                agg_type   : 'max',
                title      : 'column_a',
                name       : 'column_a',
                is_visible : true,
                index      : 0
            },
            p_column_b: {
                sort_type  : 'alpha',
                agg_type   : 'max',
                title      : 'column_b',
                name       : 'column_b',
                is_visible : true,
                index      : 1
            },
            p_column_c: {
                sort_type  : 'alpha',
                agg_type   : 'min',
                title      : 'column_c',
                name       : 'column_c',
                is_visible : true,
                index      : 2
            }
        });

        start();
    }).finish();
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
                sort_type  : 'alpha',
                agg_type   : 'max',
                title      : 'column_a',
                name       : 'column_a',
                is_visible : true,
                index      : 0
            },
            'column_b': {
                sort_type  : 'alpha',
                agg_type   : 'max',
                title      : 'column_b',
                name       : 'column_b',
                is_visible : true,
                index      : 1
            },
            'column_c': {
                sort_type  : 'alpha',
                agg_type   : 'max',
                title      : 'column_c',
                name       : 'column_c',
                is_visible : true,
                index      : 2
            }
        });
        start();
    }).finish();
});

asyncTest('alter column name (fails if changing to already existing column name)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).on_error(function (error) {
        equal(error, 'Column column_b already exists in the dataset.');

        start();
    });

    d.alter_column_name('column_a', 'column_b').finish();
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
    }).finish();
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
    }).finish();
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
    }).finish();
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
    }).finish();
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
    }).finish();
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
    }).finish();
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
    }).finish();
});

asyncTest('get partitioned (single field)', function () {
    expect(5);

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
        apple_partition  = [],
        banana_partition = [],
        cat_partition    = [],
        gummy_partition  = [];

    d.render(function () {
        deepEqual(partition_keys.sort(), [
            [ 'apple'  ],
            [ 'banana' ],
            [ 'cat'    ],
            [ 'gummy'  ]
        ]);

        deepEqual(
            apple_partition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ 'apple',   'body',    'key' ],
                [ 'apple', 'trance', 'camaro' ],
                [ 'apple', 'violin',  'music' ]
            ]
        );

        deepEqual(
            banana_partition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ 'banana', 'eyedrops', 'tie' ],
                [ 'banana',    'piano', 'gum' ]
            ]
        );

        deepEqual(
            cat_partition.sort(function (a, b) {
                if (a[1] === b[1]) return 0;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
            }),
            [
                [ 'cat',    'soy', 'blender' ],
                [ 'cat', 'tissue',     'dog' ]
            ]
        );

        deepEqual(gummy_partition, [
            [ 'gummy', 'power', 'star' ]
        ]);

        start();
    });

    d.get_partition_keys(function (keys) {
        partition_keys = keys;
    });

    d.get_partitioned(function (partition) {
        apple_partition = partition;
    }, 'apple');

    d.get_partitioned(function (partition) {
        banana_partition = partition;
    }, 'banana');

    d.get_partitioned(function (partition) {
        cat_partition = partition;
    }, 'cat');

    d.get_partitioned(function (partition) {
        gummy_partition = partition;
    }, 'gummy');

    var wait = function () {
        if (
            apple_partition.length > 0
            && banana_partition.length > 0
            && cat_partition.length > 0
            && gummy_partition.length > 0
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
});

asyncTest('get partitioned (multi-field)', function () {
    expect(7);

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
        apple_trance_partition = [],
        apple_violin_partition = [],
        banana_piano_partition = [],
        cat_soy_partition      = [],
        cat_tissue_partition   = [],
        gummy_power_partition  = [];

    d.render(function () {
        deepEqual(partition_keys.sort(), [
            [ 'apple', 'trance' ],
            [ 'apple', 'violin' ],
            [ 'banana', 'piano' ],
            [ 'cat',      'soy' ],
            [ 'cat',   'tissue' ],
            [ 'gummy', 'power'  ]
        ]);

        deepEqual(apple_trance_partition, [
            [ 'apple', 'trance', 'camaro' ]
        ]);

        deepEqual(
            apple_violin_partition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ 'apple', 'violin',   'key' ],
                [ 'apple', 'violin', 'music' ]
            ]
        );

        deepEqual(
            banana_piano_partition.sort(function (a, b) {
                if (a[2] === b[2]) return 0;
                if (a[2] < b[2]) return -1;
                if (a[2] > b[2]) return 1;
            }),
            [
                [ 'banana', 'piano', 'gum' ],
                [ 'banana', 'piano', 'tie' ]
            ]
        );

        deepEqual(cat_soy_partition, [
            [ 'cat', 'soy',  'blender' ]
        ]);

        deepEqual(cat_tissue_partition, [
            [ 'cat', 'tissue', 'dog' ]
        ]);

        deepEqual(gummy_power_partition, [
            [ 'gummy', 'power', 'star' ]
        ]);

        start();
    });

    d.get_partition_keys(function (keys) {
        partition_keys = keys;
    });

    d.get_partitioned(function (partition) {
        apple_trance_partition = partition;
    }, 'apple', 'trance');

    d.get_partitioned(function (partition) {
        apple_violin_partition = partition;
    }, 'apple', 'violin');

    d.get_partitioned(function (partition) {
        banana_piano_partition = partition;
    }, 'banana', 'piano');

    d.get_partitioned(function (partition) {
        cat_soy_partition = partition;
    }, 'cat', 'soy');

    d.get_partitioned(function (partition) {
        cat_tissue_partition = partition;
    }, 'cat', 'tissue');

    d.get_partitioned(function (partition) {
        gummy_power_partition = partition;
    }, 'gummy', 'power');

    var wait = function () {
        if (
            apple_trance_partition.length > 0
            && apple_violin_partition.length > 0
            && banana_piano_partition.length > 0
            && cat_soy_partition.length > 0
            && cat_tissue_partition.length > 0
            && gummy_power_partition.length > 0
        ) {
            d.render().finish();
        } else {
            setTimeout(wait, 0);
        }
    };

    setTimeout(wait, 0);
});

asyncTest('clone', function () {
    expect(3);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.clone(function (clone) {
        ok(clone instanceof JData);

        clone.get_columns_and_records(function (columns, records) {
            deepEqual(columns, {
                column_a: {
                    sort_type  : 'alpha',
                    agg_type   : 'max',
                    title      : 'column_a',
                    name       : 'column_a',
                    is_visible : true,
                    index      : 0
                },
                column_b: {
                    sort_type  : 'alpha',
                    agg_type   : 'max',
                    title      : 'column_b',
                    name       : 'column_b',
                    is_visible : true,
                    index      : 1
                },
                column_c: {
                    sort_type  : 'alpha',
                    agg_type   : 'max',
                    title      : 'column_c',
                    name       : 'column_c',
                    is_visible : true,
                    index      : 2
                }
            });

            deepEqual(records, [
                [ 'apple',      'violin',    'music' ],
                [ 'cat',        'tissue',      'dog' ],
                [ 'banana',      'piano',      'gum' ],
                [ 'gummy',       'power',     'star' ]
            ]);

            start();
        });
    });
});

asyncTest('get rows (all)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',     'star' ]
        ]);

        start();
    });
});

asyncTest('get rows (specify start)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',     'star' ]
        ]);

        start();
    }, 2);
});

asyncTest('get rows (specify end)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ]
        ]);

        start();
    }, undefined, 1);
});

asyncTest('get rows (specify start and end)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ]
        ]);

        start();
    }, 1, 2);
});

asyncTest('get rows (specify a too-large end)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',     'star' ]
        ]);

        start();
    }, 1, 10);
});

asyncTest('get rows (specify columns)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'cat',     'dog' ],
            [ 'banana',  'gum' ],
            [ 'gummy',  'star' ]
        ]);

        start();
    }, 1, 10, 'column_a', 'column_c');
});

asyncTest('get rows (specify columns as array)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'cat',     'dog' ],
            [ 'banana',  'gum' ],
            [ 'gummy',  'star' ]
        ]);

        start();
    }, 1, 10, [ 'column_a', 'column_c' ]);
});

asyncTest('get rows (specify out-of-order columns)', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_rows(function (result) {
        deepEqual(result, [
            [ 'dog', 'cat' ]
        ]);

        start();
    }, 1, 1, 'column_c', 'column_a');
});

asyncTest('get number of records', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.get_number_of_records(function (result) {
        equal(result, 4);
        start();
    });
});

asyncTest('get_rows obeys applied filter', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.apply_filter(/apple/);

    d.get_rows(function(result) {
        deepEqual(result, [
            [ 'apple', 'violin', 'music' ],
        ]);
        start();
    }).finish();
});

asyncTest('get_columns_and_records obeys applied filter', function () {
    expect(1);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.apply_filter(/apple/);

    d.get_columns_and_records(function(columns, rows) {
        deepEqual(rows, [
            [ 'apple', 'violin', 'music' ],
        ]);
        start();
    }).finish();
});

asyncTest('set_decimal_mark_character', function () {
    expect(1);

    var dataset = [
        [ 'column_a' ],

        [ "1.435"    ],
        [ "3,600"    ],
        [ "4.56"     ],
        [ "2,345"    ]
    ];

    var d = new JData(dataset);

    d.alter_column_sort_type('column_a', 'num')
     .sort('column_a')
     .get_columns_and_records(function (columns, rows) {
        deepEqual(rows, [
            [ "1.435" ],
            [ "4.56"  ],
            [ "2,345" ],
            [ "3,600" ]
        ]);
        start();
     }).finish();
});

asyncTest('get_distinct_consecutive_rows', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b' ],

        [ 'abc',      '123'      ],
        [ 'abc',      '456'      ],
        [ 'abc',      '789'      ],
        [ 'def',      '123'      ],
        [ 'ghi',      '123'      ],
        [ 'ghi',      '456'      ],
        [ 'def',      '456'      ],
        [ 'def',      '789'      ]
    ];

    var d = new JData(dataset);

    d.alter_column_sort_type('column_a', 'num')
     .sort('column_a')
     .get_distinct_consecutive_rows(function (rows) {
        deepEqual(rows, [
            [ 'abc', 0, 2 ],
            [ 'def', 3, 3 ],
            [ 'ghi', 4, 5 ],
            [ 'def', 6, 7 ]
        ]);
     }, 'column_a').get_distinct_consecutive_rows(function (rows) {
        deepEqual(rows, [
            [ '123', 0, 0 ],
            [ '456', 1, 1 ],
            [ '789', 2, 2 ],
            [ '123', 3, 4 ],
            [ '456', 5, 6 ],
            [ '789', 7, 7 ]
        ]);
        start();
     }, 'column_b').finish();
});

asyncTest('extra_column_info_gets_passed_along', function () {
    expect(2);

    var dataset = [
        [
            {
                name         : 'column_a',
                abc          : 'xyz',
                random_stuff : 'still here'
            },
            {
                name      : 'column_b',
                elephants : 'donkeys'
            }
        ],

        [ 'apple',      'violin' ],
        [ 'cat',        'tissue' ],
        [ 'banana',      'piano' ],
        [ 'gummy',       'power' ]
    ];

    var d = new JData(dataset);

    ok(d instanceof JData);

    d.get_columns(function (columns) {
        deepEqual(columns, {
            column_a: {
                sort_type    : 'alpha',
                agg_type     : 'max',
                title        : 'column_a',
                name         : 'column_a',
                is_visible   : true,
                index        : 0,
                abc          : 'xyz',
                random_stuff : 'still here'
            },
            column_b: {
                sort_type    : 'alpha',
                agg_type     : 'max',
                title        : 'column_b',
                name         : 'column_b',
                is_visible   : true,
                index        : 1,
                elephants    : 'donkeys'
            }
        });

        start();
    }).finish();
});

asyncTest('hide columns (single)', function () {
    expect(4);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d.hide_columns('column_a').get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 0
            },
            column_c: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_c',
                name           : 'column_c',
                is_visible     : true,
                index          : 1
            }
        });

        deepEqual(records, [
            [ 'violin',    'music' ],
            [ 'tissue',      'dog' ],
            [  'piano',      'gum' ],
            [  'power',     'star' ]
        ]);
    }).get_all_columns_and_all_records(function (columns, records) {
        deepEqual(columns, {
            column_a: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : false,
                index          : 0
            },
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 1
            },
            column_c: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_c',
                name           : 'column_c',
                is_visible     : true,
                index          : 2
            }
        });

        deepEqual(records, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',     'star' ]
        ]);

        start();
    }).finish();
});

asyncTest('hide columns (multi)', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns('column_a', 'column_c');

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 0
            }
        });

        deepEqual(records, [
            [ 'violin' ],
            [ 'tissue' ],
            [  'piano' ],
            [  'power' ]
        ]);

        start();
    }).finish();
});

asyncTest('hide column that does not exist does not error out', function () {
    expect(0);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).render(function () { start(); })
                              .hide_columns('column_d')
                              .render()
                              .finish();
});

asyncTest('hide columns (regex)', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'notcolumn_b', 'column_c' ],

        [ 'apple',         'violin',    'music' ],
        [ 'cat',           'tissue',      'dog' ],
        [ 'banana',         'piano',      'gum' ],
        [ 'gummy',          'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns(/^column/);

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            notcolumn_b : {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'notcolumn_b',
                name           : 'notcolumn_b',
                is_visible     : true,
                index          : 0
            }
        });

        deepEqual(records, [
            [ 'violin' ],
            [ 'tissue' ],
            [  'piano' ],
            [  'power' ]
        ]);

        start();
    }).finish();
});

asyncTest('hide columns (regex, w/ flags)', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'notcolumn_b', 'COLumn_c' ],

        [ 'apple',         'violin',    'music' ],
        [ 'cat',           'tissue',      'dog' ],
        [ 'banana',         'piano',      'gum' ],
        [ 'gummy',          'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns(/^column/i);

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            notcolumn_b : {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'notcolumn_b',
                name           : 'notcolumn_b',
                is_visible     : true,
                index          : 0
            }
        });

        deepEqual(records, [
            [ 'violin' ],
            [ 'tissue' ],
            [  'piano' ],
            [  'power' ]
        ]);

        start();
    }).finish();
});

asyncTest('show columns', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns('column_a', 'column_c')
                              .show_columns('column_a');

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            column_a: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            },
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 1
            }
        });

        deepEqual(records, [
            [ 'apple',      'violin' ],
            [ 'cat',        'tissue' ],
            [ 'banana',      'piano' ],
            [ 'gummy',       'power' ]
        ]);

        start();
    }).finish();
});

asyncTest('show column that does not exist does not error out', function () {
    expect(0);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).render(function () { start(); })
                              .show_columns('column_d')
                              .render()
                              .finish();
});

asyncTest('show columns (regex)', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'notcolumn_b', 'COLumn_c' ],

        [ 'apple',         'violin',    'music' ],
        [ 'cat',           'tissue',      'dog' ],
        [ 'banana',         'piano',      'gum' ],
        [ 'gummy',          'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns('column_a', 'notcolumn_b', 'COLumn_c')
                              .show_columns(/^(?:not)?column_/);

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            column_a: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            },
            notcolumn_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'notcolumn_b',
                name           : 'notcolumn_b',
                is_visible     : true,
                index          : 1
            }
        });

        deepEqual(records, [
            [ 'apple',      'violin' ],
            [ 'cat',        'tissue' ],
            [ 'banana',      'piano' ],
            [ 'gummy',       'power' ]
        ]);

        start();
    }).finish();
});

asyncTest('hide all columns' , function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns('column_a', 'column_c')
                              .hide_all_columns();

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {});
        deepEqual(records, [ [], [], [], [] ]);

        start();
    }).finish();
});

asyncTest('show all columns', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset).hide_columns('column_a', 'column_c')
                              .show_all_columns();

    d.get_columns_and_records(function (columns, records) {
        deepEqual(columns, {
            column_a: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_a',
                name           : 'column_a',
                is_visible     : true,
                index          : 0
            },
            column_b: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_b',
                name           : 'column_b',
                is_visible     : true,
                index          : 1
            },
            column_c: {
                sort_type      : 'alpha',
                agg_type       : 'max',
                title          : 'column_c',
                name           : 'column_c',
                is_visible     : true,
                index          : 2
            }
        });

        deepEqual(records, [
            [ 'apple',      'violin',    'music' ],
            [ 'cat',        'tissue',      'dog' ],
            [ 'banana',      'piano',      'gum' ],
            [ 'gummy',       'power',     'star' ]
        ]);

        start();
    }).finish();
});

asyncTest('changes for "on_" functions are added to the queue by default', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d._action_queue._isInAction = true;

    d.on_error(function (error) {
        equal(error, 'Column column_b already exists in the dataset.');

        d.finish();
        start();
    });

    equal(d._action_queue._queue.length, 1);

    d.alter_column_name('column_a', 'column_b');

    d._finish_action();
});

asyncTest('changes for "on_" functions can happen immediately with a flag', function () {
    expect(2);

    var dataset = [
        [ 'column_a', 'column_b', 'column_c' ],

        [ 'apple',      'violin',    'music' ],
        [ 'cat',        'tissue',      'dog' ],
        [ 'banana',      'piano',      'gum' ],
        [ 'gummy',       'power',     'star' ]
    ];

    var d = new JData(dataset);

    d._action_queue._isInAction = true;

    d.on_error(function (error) {
        equal(error, 'Column column_b already exists in the dataset.');

        d.finish();
        start();
    }, true);

    equal(d._action_queue._queue.length, 0);

    d.alter_column_name('column_a', 'column_b');

    d._finish_action();
});
