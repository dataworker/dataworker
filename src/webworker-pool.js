(function () {
    "use strict";

    var WWP = window.WebWorkerPool = function WebWorkerPool(src) {
        var self = this instanceof WWP ? this : Object.create(WWP.prototype);

        self._src     = src;
        self._workers = [];

        return self;
    };

    if (typeof(window) === "undefined" || window.Worker === undefined) return WWP;

    WWP.prototype.getWorker = function () {
        return this._workers.length ? this._workers.pop() : new Worker(this._src);
    };

    WWP.prototype.reclaim = function (worker) {
        this._workers.push(worker)
    };
})();
