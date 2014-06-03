(function (global) {
    "use strict";

    var READY_STATE = {
        CONNECTING : 0,
        OPEN       : 1,
        CLOSING    : 2,
        CLOSED     : 3
    };

    var MockWebSocket = global.WebSocket = function (source) {
        var self = this;

        self.readyState = READY_STATE.CONNECTING;

        self.onopen    = function () {};
        self.onclose   = function () {};
        self.onerror   = function () {};
        self.onmessage = function () {};

        if (source !== MockWebSocket.expectedSource) {
            MockWebSocket.unexpected(
                "Unexpected:"
                + "\n\tExpected: " + self.expectedSource
                + "\n\tGot: " + source
            );

            return self;
        }

        setTimeout(function () {
            self.readyState = READY_STATE.OPEN;
            self.onopen();
        });

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
                    setTimeout(function () {
                        self.onmessage({ data: replyMsg });
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
            self.onclose();
        });
    };

    MockWebSocket.expectedSource  = undefined;
    MockWebSocket.expectedReplies = undefined;

    MockWebSocket.unexpected = function () {};
})(this);
