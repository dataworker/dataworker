(function () {
    "use strict";

    if (typeof window === "undefined") {
        return;
    }

    var ActionQueue = window.ActionQueue = function () {
        var self = this instanceof ActionQueue ? this : Object.create(ActionQueue.prototype);

        self._queueStack     = [];
        self._stackIndex     = 0;

        self._isInAction      = false;
        self._isOffEventQueue = false;

        self._previousAction = undefined;

        self._isFinished = false;

        return self;
    };

    ActionQueue.prototype.queueNext = function () {
        var self = this, args = Array.prototype.slice.call(arguments),
            stackIndex = self._isOffEventQueue ? 0 : self._stackIndex;

        if (self._isFinished) return self;

        if (typeof(self._queueStack[stackIndex]) === "undefined") {
            self._queueStack[stackIndex] = [];
        }

        self._queueStack[stackIndex].push(args);
        self._nextAction();

        return self;
    };

    ActionQueue.prototype._nextAction = function (finishPrevious) {
        var self = this, args, action, queue;

        if (finishPrevious) self._isInAction = false;

        if (!self._isInAction) {
            (function doNextAction() {
                if (self._isFinished) return;

                queue = self._queueStack[self._stackIndex];

                if (queue && queue.length > 0) {
                    args   = queue.shift();
                    action = args.shift();

                    if (queue.length === 0) {
                        self._queueStack.splice(self._stackIndex, 1);
                    }

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

    ActionQueue.prototype.beginAsynchronous = function () {
        var self =  this;

        self._isOffEventQueue = true;

        return self;
    };

    ActionQueue.prototype.finishAsynchronous = function () {
        var self =  this;

        self._isOffEventQueue = false;

        return self;
    };

    ActionQueue.prototype.finishAction = function () {
        var self = this;

        self._nextAction(true);

        return self;
    };

    ActionQueue.prototype.finish = function () {
        var self = this;

        self.queueNext(function () {
            self._previousAction = null;
            self._queueStack     = null;

            self._isFinished = true;

            self.finishAction();
        });

        return self;
    };

    ActionQueue.prototype.printToConsole = function () {
        var self = this, h1 = "color: #D48919; font-weight: bold;", h2 = "color: #D48919;";

        console.group("Action Queue [" + (self._isInAction ? "IN ACTION" : "RESTING") + "]");

        if (typeof(self._previousAction) !== "undefined") {
            console.groupCollapsed(
                "%c" + (self._isInAction ? "Current action:" : "Previous action:"),
                h1
            );
            console.log("" + self._previousAction.toString());
            console.groupEnd();
        }

        console.groupCollapsed("%cQueue stack [" + self._queueStack.length + "]:", h1);

        self._queueStack.forEach(function (queue, queueNum) {
            console.groupCollapsed("%cQueue " + queueNum + " [" + queue.length + "]:", h2);

            queue.forEach(function (action, actionNum) {
                if (typeof(action) !== "undefined") {
                    console.log(actionNum + ": " + action.toString());
                }
            });

            console.groupEnd();
        });

        console.groupEnd();
        console.groupEnd();

        return self;
    };
})();
