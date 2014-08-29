/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 *                                                                           *
 * Tests for WebWorkerPool                                                   *
 *                                                                           *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

module("WebWorkerPool");

asyncTest("creates webworker out of simple source file", function () {
    expect(2);

    var wwp    = new WebWorkerPool("resources/counting-webworker.js"),
        worker = wwp.getWorker(),
        count  = 0;

    worker.onmessage = function (a) {
        equal(a.data.numMessages, 1);
        worker.onmessage = function (b) {
            equal(b.data.numMessages, 2);

            worker.terminate();
            start();
        }
        worker.postMessage({});
    };

    worker.postMessage({});
});

asyncTest("creates webworker from Blob, if browser supports Blobs", function () {
    var url;

    try {
        var response = "this.onmessage=function (e) { postMessage({ square: e.data * e.data }) }",
            blob     = new Blob([ response ], { type: "application/javascript" });

        url = URL.createObjectURL(blob);
    } catch (ignore) { }

    if (url) {
        expect(2);

        var wwp = new WebWorkerPool(url),
            worker = wwp.getWorker();

        URL.revokeObjectURL(url);

        worker.onmessage = function (a) {
            equal(a.data.square, 49);
            worker.onmessage = function (b) {
                equal(b.data.square, 1);

                worker.terminate();
                start();
            }
            worker.postMessage(1);
        };

        worker.postMessage(7);
    } else {
        expect(0);
        start();
    }
});

asyncTest("new webworker is created unless one has been reclaimed", function () {
    expect(5);

    var wwp = new WebWorkerPool("resources/counting-webworker.js"),
        worker1, worker2, worker3;

    function step1() {
        var count = 0;

        worker1 = wwp.getWorker();
        worker2 = wwp.getWorker();

        worker1.onmessage = worker2.onmessage = function (e) {
            equal(e.data.numMessages, 1);

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

        worker2 = wwp.getWorker();
        worker3 = wwp.getWorker();

        worker1.onmessage = worker2.onmessage = function (e) {
            equal(e.data.numMessages, 2);

            if (++count === 3) finishAll();
        }

        worker3.onmessage = function (e) {
            equal(e.data.numMessages, 1);

            if (++count === 3) finishAll();
        }

        worker1.postMessage({});
        worker2.postMessage({});
        worker3.postMessage({});
    }

    function finishAll() {
        worker1.terminate();
        worker2.terminate();
        worker3.terminate();

        start();
    }

    step1();
});

asyncTest("reuses webworker from Blob, if browser supports Blobs", function () {
    var url;

    try {
        var response = "var count = 0; this.onmessage=function (e) { postMessage(++count) }",
            blob     = new Blob([ response ], { type: "application/javascript" });

        url = URL.createObjectURL(blob);
    } catch (ignore) { }

    if (url) {
        expect(4);

        var wwp     = new WebWorkerPool(url),
            worker1 = wwp.getWorker(),
            worker2;

        function finishAll() {
            worker1.terminate();
            worker2.terminate();

            URL.revokeObjectURL(url);

            start();
        }

        worker1.onmessage = function (a) {
            equal(a.data, 1);

            wwp.reclaim(worker1);
            worker2 = wwp.getWorker();

            worker2.onmessage = function (b) {
                equal(b.data, 2);

                var count = 0;

                wwp.reclaim(worker2);
                worker1 = wwp.getWorker();
                worker2 = wwp.getWorker();

                worker1.onmessage = function (c) {
                    equal(c.data, 3);
                    if (count++) finishAll();
                };

                worker2.onmessage = function (c) {
                    equal(c.data, 1);
                    if (count++) finishAll();
                };

                worker1.postMessage({});
                worker2.postMessage({});
            };

            worker2.postMessage({});
        };

        worker1.postMessage({});
    } else {
        expect(0);
        start();
    }
});
