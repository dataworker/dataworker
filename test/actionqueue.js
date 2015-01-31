QUnit.module("ActionQueue");

QUnit.test("construct", function (assert) {
    var q = new ActionQueue();

    assert.ok(q instanceof ActionQueue);
});

QUnit.test("queue action (not in action)", function (assert) {
    var q = new ActionQueue(), actionDone = false;

    q.queueNext(function () {
        actionDone = true;
        q.finishAction();
    });

    assert.ok(actionDone);
});

QUnit.test("queue action (in action)", function (assert) {
    var q = new ActionQueue();

    function toQ() { q.finishAction(); };

    q._isInAction = true;
    q.queueNext(toQ);

    assert.deepEqual(
        q._queueStack,
        [
            [
                [ toQ ]
            ]
        ]
    );
});

QUnit.test("queue action (named function with args)", function (assert) {
    var q = new ActionQueue(), toSet = "arghh";

    function toQ(arg) {
        toSet = arg;
        q.finishAction();
    };

    q.queueNext(toQ, "Hello, world!");

    assert.equal(toSet, "Hello, world!");
});

QUnit.test("queue action (within queued action)", function (assert) {
    expect(6);

    var done = assert.async();

    var q = new ActionQueue(), steps = 0;

    q._isInAction = true;

    q.queueNext(function () {
        assert.equal(steps++, 0);

        q.queueNext(function () {
            assert.equal(steps++, 1);

            q.queueNext(function () {
                assert.equal(steps++, 2);
                setTimeout(function () { q.finishAction(); }, 50);
            });

            setTimeout(function () { q.finishAction(); }, 10);
        });

        q.finishAction();
    }).queueNext(function () {
        assert.equal(steps++, 3);

        q.queueNext(function () {
            assert.equal(steps++, 4);

            q.finishAction();
        });

        q.finishAction();
    }).queueNext(function () {
        assert.equal(steps++, 5);

        q.finishAction();
        done();
    });

    q.finishAction();
});

QUnit.test(
    "queue an action while an executing action is waiting for async response",
    function (assert) {
        expect(7);

        var done = assert.async();

        var q = new ActionQueue(), steps = 0;

        q.queueNext(function () {
            assert.equal(steps++, 0, "first step");


            q.beginAsynchronous();
            setTimeout(function () {
                assert.equal(steps++, 1, "second step");

                q.finishAsynchronous();
                q.finishAction();
            }, 100);
        });

        setTimeout(function () {
            assert.equal(steps++, 3, "fourth step");

            q.queueNext(function () {
                assert.equal(steps++, 6, "seventh step");
                q.finishAction();
                done();
            });
        }, 200);

        q.queueNext(function () {
            assert.equal(steps++, 2, "third step");

            q.beginAsynchronous();
            setTimeout(function () {
                assert.equal(steps++, 4, "fifth step");

                q.finishAsynchronous();
                q.finishAction();
            }, 400);
        }).queueNext(function () {
            assert.equal(steps++, 5, "sixth step");
            q.finishAction();
        });
    }
);


QUnit.test(
    "queue an action while executing action awaits async reponse works on stack index > 1",
    function (assert) {
        expect(9);

        var done = assert.async();

        var q = new ActionQueue(), steps = 1;

        function one(step) {
            q.queueNext(function () {
                assert.equal(steps++, step, "step " + step);

                q.beginAsynchronous();
                setTimeout(function () {
                    q.finishAsynchronous();
                    q.finishAction();
                }, 100);
            });
        }

        function two(step) {
            q.queueNext(function () {
                assert.equal(steps++, step, "step " + step);

                if (step == 2) three();
                q.finishAction();

                if (step == 9) done();
            });
        }

        function three() {
            q.queueNext(function () {
                assert.equal(steps++, 3, "step 3");

                q.beginAsynchronous();
                setTimeout(function () {
                    assert.equal(steps++, 5, "step 5");

                    q.finishAsynchronous();
                    q.finishAction();
                }, 300);
            });
        }

        one(1);
        two(2);
        one(6);
        two(7);

        setTimeout(function () {
            assert.equal(steps++, 4, "step 4");

            one(8);
            two(9);
        }, 200);
    }
);

QUnit.test("finish action", function (assert) {
    var q = new ActionQueue(),
        action1Done = false, action2Done = false;

    function toQ1() {
        action1Done = true;
        q.finishAction();
    }
    function toQ2() {
        action2Done = true;
        q.finishAction();
    }

    q._isInAction = true;

    q.queueNext(toQ1).queueNext(toQ2).finishAction();

    assert.ok(action1Done);
    assert.ok(action2Done);
});

QUnit.test("do not allow any more inputs after action queue is disposed", function (assert) {
    var q = new ActionQueue(),
        action1Done = false, action2Done = false;

    function toQ1() {
        action1Done= true;
        q.finishAction();
    }
    function toQ2() {
        action2Done = true;
        q.finishAction();
    }

    q._isInAction = true;

    q.queueNext(toQ1)
     .finish()
     .queueNext(toQ2)
     .finishAction();

    assert.ok(action1Done);
    assert.ok(!action2Done);
});
