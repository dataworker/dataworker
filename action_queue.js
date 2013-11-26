(function () {
    "use strict";

    var ActionQueue = window.ActionQueue = function () {
        var self = this instanceof ActionQueue ? this : Object.create(ActionQueue.prototype);

        self._queueStack     = [];
        self._stackIndex     = 0;

        self._isInAction     = false;

        self._previousAction = undefined;

        return self;
    };

    ActionQueue.prototype.queueNext = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        if (typeof(self._queueStack[self._stackIndex]) === "undefined") {
            self._queueStack[self._stackIndex] = [];
        }

        self._queueStack[self._stackIndex].push(args);
        self._nextAction();

        return self;
    };

    ActionQueue.prototype._nextAction = function (finishPrevious) {
        var self = this, args, action, queue = self._queueStack[self._stackIndex];

        if (finishPrevious) self._isInAction = false;

        if (!self._isInAction) {
            (function doNextAction() {
                queue = self._queueStack[self._stackIndex];

                if (queue && queue.length > 0) {
                    args   = queue.shift();
                    action = args.shift();

                    self._isInAction = true;
                    self._previousAction = action;

                    self._stackIndex++;
                    self._callAction(function () { action.apply(self, args) });
                } else if (self._stackIndex > 0) {
                    self._stackIndex--;
                    doNextAction();
                }
            })();
        }

        return self;
    };

    ActionQueue.prototype._callAction = function (func) { func(); };

    ActionQueue.prototype.finishAction = function () {
        var self = this;

        self._nextAction(true);

        return self;
    };
})();
