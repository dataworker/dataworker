(function () {
    "use strict";

    var WWP = window.WebWorkerPool = function WebWorkerPool(optionalSource) {
        var self = this instanceof WWP ? this : Object.create(WWP.prototype);

        self._workers = {};
        self._src     = optionalSource;

        return self;
    };

    if (typeof(window) === "undefined" || window.Worker === undefined) return WWP;

    WWP.prototype.getWorker = function (sourceList) {
        var self    = this,
            sources = [].concat(sourceList || self._src || []),
            worker;

        sources.some(function (src) {
            var workers = self._workers[src];
            if (workers === "ignore") return false;

            if (worker = workers && workers.length && workers.pop()) return true;

            try {
                worker = new Worker(src);
                worker.source = src;

                self._workers[src] = [];
                return true;
            } catch (error) {
                self._workers[src] = "ignore";
                return false;
            }
        });

        return worker;
    };

    WWP.prototype.reclaim = function (worker) {
        if (worker && worker.source) this._workers[worker.source].push(worker);
    };
})();
