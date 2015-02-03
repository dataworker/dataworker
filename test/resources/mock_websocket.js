(function (global) {
    "use strict";

    var READY_STATE = {
        CONNECTING : 0,
        OPEN       : 1,
        CLOSING    : 2,
        CLOSED     : 3
    }, _this;

    var MockWebSocket = global.WebSocket = function (source) {
        var self = _this = this;

        self.readyState = READY_STATE.CONNECTING;

        self.onopen    = function () {};
        self.onclose   = function () {};
        self.onerror   = function () {};
        self.onmessage = function () {};

        self._replyMessages = [];

        if (source !== MockWebSocket.expectedSource) {
            MockWebSocket.unexpected(
                "Unexpected:"
                + "\n\tExpected: " + MockWebSocket.expectedSource
                + "\n\tGot: " + source
            );

            return self;
        }

        setTimeout(function () {
            self.readyState = READY_STATE.OPEN;
            self.onopen();
        });

        if ((self.interruptAfter || MockWebSocket.interruptAfter) !== undefined) {
            setTimeout(function () { self.interrupt(); }, source.interruptAfter);
        }

        return self;
    };

    MockWebSocket.prototype.send = function (msg) {
        var self = this;

        if (typeof(msg) !== "string") msg = JSON.stringify(msg);

        if (msg in MockWebSocket.expectedReplies) {
            var reply = MockWebSocket.expectedReplies[msg];

            if (typeof(reply) === "function") reply = reply();
            if (!(reply instanceof Array))    reply = [ reply ];

            reply.forEach(function (replyMsg) {
                if (replyMsg !== undefined) {
                    self._replyMessages.push(replyMsg);
                    setTimeout(function () {
                        self.onmessage({ data: self._replyMessages.shift() });
                    });
                }
            });
        } else {
            MockWebSocket.unexpected("Unexpected message: " + msg);
        }
    };

    MockWebSocket.prototype.close = function () {
        var self = this;

        self.readyState = READY_STATE.CLOSING;

        setTimeout(function () {
            self.readyState = READY_STATE.CLOSED;
            self.onclose({ code: 1000 });
        });
    };

    MockWebSocket.setInterrupt = function (interruptAfter) {
        setTimeout(function () {
            _this.readyState = READY_STATE.CLOSED;
            _this.onclose({ code: 4000 });
        }, interruptAfter);
    };

    MockWebSocket.interruptAfter  = undefined;
    MockWebSocket.expectedSource  = undefined;
    MockWebSocket.expectedReplies = undefined;

    MockWebSocket.unexpected = function () {};
})(this);
