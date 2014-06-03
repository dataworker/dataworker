var count = 0;

this.onmessage = function (msg) {
    this.postMessage({ numMessages: ++count });
};
