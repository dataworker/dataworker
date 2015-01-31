var count = 0;

this.onmessage = function (msg) {
    if (msg.data === "reset") {
        count = 0;
    } else {
        this.postMessage({ numMessages: ++count });
    }
};
