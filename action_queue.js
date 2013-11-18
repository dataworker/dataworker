(function () {
    "use strict";

    var ActionQueue = window.ActionQueue = function (parentQueue) {
        var self = this instanceof ActionQueue ? this : Object.create(ActionQueue.prototype);

        self._queue          = [];
        self._isInAction     = false;
        self._previousAction = undefined;

        self._parentQueue    = parentQueue;

        return self;
    };

    ActionQueue.prototype.queueNext = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue.push(args);
        self._nextAction();

        return self;
    };

    ActionQueue.prototype._nextAction = function () {
        var self = this;

        if (!self._isInAction) {
            if (self._queue.length > 0) {
                var args   = self._queue.shift(),
                    action = args.shift();

                self._isInAction = true;
                self._previousAction = action;

                self._callAction(function () { action.apply(new ActionQueue(self), args) });
            } else if (typeof(self._parentQueue) !== "undefined") {
                self._parentQueue.finishAction();
            }
        }

        return self;
    };

    ActionQueue.prototype._callAction = function(func) { func(); };

    ActionQueue.prototype.finishAction = function () {
        var self = this;

        self._isInAction = false;
        self._nextAction();

        return self;
    };
})();
