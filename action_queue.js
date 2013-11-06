(function () {
    "use strict";

    var ActionQueue = window.ActionQueue = function () {
        var self = this instanceof ActionQueue ? this : Object.create(ActionQueue.prototype);

        self._queue          = [];
        self._isInAction     = false;
        self._previousAction = undefined;

        return self;
    };

    ActionQueue.prototype.queueNext = function (action) {
        var self = this;

        self._actionQueue.push(action);
        self._nextAction();

        return self;
    };

    ActionQueue.prototype.nextAction = function (finishPrevious) {
        var self = this;

        if (finishPrevious) self._isInAction = false;

        if (!self._isInAction && self._actionQueue.length > 0) {
            var action = self._actionQueue.shift();

            self._isInAction = true;
            self._previousAction = action;

            action();
        }

        return self;
    };

    ActionQueue.prototype.finishAction = function () {
        var self = this;

        self.nextAction(true);

        return self;
    };
})();
