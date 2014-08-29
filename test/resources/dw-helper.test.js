(function (globalWorker) {
    "use strict";

    importScripts(
        "mock_websocket.js", // Overrides WebSocket w/ a mock object
        "../../src/dw-helper.js"
    );

    WebSocket.unexpected = function (msg) {
        // Pass the message back to the client as an error.
        this.onmessage({ error: msg })
    };

    var onmessageHandler = globalWorker.onmessage;
    globalWorker.onmessage = function (e) {
        if ("meta" in e.data) {
            var meta = e.data.meta;

            if ("expectedSource" in meta)  WebSocket.expectedSource = meta.expectedSource;
            if ("expectedReplies" in meta) WebSocket.expectedReplies = meta.expectedReplies;
        } else {
            onmessageHandler(e);
        }
    };
})(this);