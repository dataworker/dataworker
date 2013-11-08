(function () {
    "use strict";

    var ActionQueue = window.ActionQueue = function () {
        var self = this instanceof ActionQueue ? this : Object.create(ActionQueue.prototype);

        self._queue          = [];
        self._isInAction     = false;
        self._previousAction = undefined;

        return self;
    };

    ActionQueue.prototype.queueNext = function () {
        var self = this,
            args = [];

        Array.prototype.push.apply(args, arguments);

        self._queue.push(args);
        nextAction.call(self);

        return self;
    };

    function nextAction (finishPrevious) {
        var self = this;

        if (finishPrevious) self._isInAction = false;

        if (!self._isInAction && self._queue.length > 0) {
            var args   = self._queue.shift(),
                action = args.shift();

            self._isInAction = true;
            self._previousAction = action;

            action.apply(self, args);
        }

        return self;
    };

    ActionQueue.prototype.finishAction = function () {
        var self = this;

        nextAction.call(self, true);

        return self;
    };
})();
