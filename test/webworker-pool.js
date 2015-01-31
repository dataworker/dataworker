var wwp = new WebWorkerPool();

QUnit.module("WebWorkerPool");
QUnit.moduleDone(function (details) {
    if (details.name === "WebWorkerPool") {
        var worker;
        while (worker = wwp.getWorker("resources/counting-webworker.js", true)) {
            worker.terminate();
        }
    }
});

QUnit.test("creates webworker out of simple source file", function (assert) {
    assert.expect(2);

    var done = assert.async();

    var worker = wwp.getWorker("resources/counting-webworker.js"),
        count  = 0;

    worker.onmessage = function (a) {
        assert.equal(a.data.numMessages, 1);
        worker.onmessage = function (b) {
            assert.equal(b.data.numMessages, 2);

            worker.postMessage("reset");
            wwp.reclaim(worker);
            done();
        }
        worker.postMessage({});
    };

    worker.postMessage({});
});

QUnit.test("creates webworker from Blob, if browser supports Blobs", function (assert) {
    var url;

    var done = assert.async();

    try {
        var response = "this.onmessage=function (e) { postMessage({ square: e.data * e.data }) }",
            blob     = new Blob([ response ], { type: "application/javascript" });

        url = URL.createObjectURL(blob);
    } catch (ignore) { }

    if (url) {
        assert.expect(2);

        var worker = wwp.getWorker(url);

        worker.onmessage = function (a) {
            assert.equal(a.data.square, 49);
            worker.onmessage = function (b) {
                assert.equal(b.data.square, 1);

                worker.terminate();
                URL.revokeObjectURL(url);
                done();
            }
            worker.postMessage(1);
        };

        worker.postMessage(7);
    } else {
        assert.expect(0);
        done();
    }
});

QUnit.test("new webworker is created unless one has been reclaimed", function (assert) {
    assert.expect(5);

    var url = "resources/counting-webworker.js",
        done = assert.async(),
        worker1, worker2, worker3;

    function step1() {
        var count = 0;

        worker1 = wwp.getWorker(url);
        worker2 = wwp.getWorker(url);

        worker1.onmessage = worker2.onmessage = function (e) {
            assert.equal(e.data.numMessages, 1);

            if (++count === 2) {
                wwp.reclaim(worker2);
                worker2 = null;

                step2();
            }
        }

        worker1.postMessage({});
        worker2.postMessage({});
    }

    function step2() {
        var count = 0;

        worker2 = wwp.getWorker(url);
        worker3 = wwp.getWorker(url);

        worker1.onmessage = worker2.onmessage = function (e) {
            assert.equal(e.data.numMessages, 2);

            if (++count === 3) finishAll();
        }

        worker3.onmessage = function (e) {
            assert.equal(e.data.numMessages, 1);

            if (++count === 3) finishAll();
        }

        worker1.postMessage({});
        worker2.postMessage({});
        worker3.postMessage({});
    }

    function finishAll() {
        worker1.postMessage("reset");
        worker2.postMessage("reset");
        worker3.postMessage("reset");

        wwp.reclaim(worker1);
        wwp.reclaim(worker2);
        wwp.reclaim(worker3);

        done();
    }

    step1();
});

QUnit.test("reuses webworker from Blob, if browser supports Blobs", function (assert) {
    var url;

    var done = assert.async();

    try {
        var response = "var count = 0; this.onmessage=function (e) { postMessage(++count) }",
            blob     = new Blob([ response ], { type: "application/javascript" });

        url = URL.createObjectURL(blob);
    } catch (ignore) { }

    if (url) {
        assert.expect(4);

        var worker1 = wwp.getWorker(url),
            worker2;

        function finishAll() {
            worker1.terminate();
            worker2.terminate();

            URL.revokeObjectURL(url);

            done();
        }

        worker1.onmessage = function (a) {
            assert.equal(a.data, 1);

            wwp.reclaim(worker1);
            worker2 = wwp.getWorker(url);

            worker2.onmessage = function (b) {
                assert.equal(b.data, 2);

                var count = 0;

                wwp.reclaim(worker2);
                worker1 = wwp.getWorker(url);
                worker2 = wwp.getWorker(url);

                worker1.onmessage = function (c) {
                    assert.equal(c.data, 3);
                    if (count++) finishAll();
                };

                worker2.onmessage = function (c) {
                    assert.equal(c.data, 1);
                    if (count++) finishAll();
                };

                worker1.postMessage({});
                worker2.postMessage({});
            };

            worker2.postMessage({});
        };

        worker1.postMessage({});
    } else {
        assert.expect(0);
        done();
    }
});
