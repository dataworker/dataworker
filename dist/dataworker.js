/*!
 * DataWorker v3.1.0 (http://git.io/dw)
 * Copyright 2014-2015 Rentrak Corporation and other contributors
 * Licensed under MIT (https://github.com/dataworker/dataworker/blob/master/LICENSE)
 */
(function () {
    "use strict";

    if (typeof window === "undefined") {
        return;
    }

    var WWP = window.WebWorkerPool = function WebWorkerPool(optionalSource) {
        var self = this instanceof WWP ? this : Object.create(WWP.prototype);

        self._workers = {};
        self._src     = optionalSource;

        return self;
    };

    if (typeof(window) === "undefined" || window.Worker === undefined) return WWP;

    WWP.prototype.getWorker = function (sourceList, onlyReclaimed) {
        var self    = this,
            sources = [].concat(sourceList || self._src || []),
            worker;

        sources.some(function (src) {
            var workers = self._workers[src];
            if (workers === "ignore") return false;

            worker = workers && workers.length && workers.pop();
            if (worker) return true;
            if (onlyReclaimed) return false;

            try {
                worker = new Worker(src);
                worker.source = src;

                self._workers[src] = [];
                return true;
            } catch (error) {
                self._workers[src] = "ignore";
                return false;
            }
        });

        return worker;
    };

    WWP.prototype.reclaim = function (worker) {
        if (worker && worker.source) this._workers[worker.source].push(worker);
    };
})();

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
                    self._callAction(function () { action.apply(self, args); });
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

function DataWorkerHelperCreator(globalWorker) {
    "use strict";

    var helper;

    var DWH = function DataWorkerHelper() {
        var self = this instanceof DWH ? this : Object.create(DWH.prototype);

        self._columns     = {};
        self._rows        = [];
        self._summaryRows = [];

        self._partitionedBy   = [];
        self._partitionedRows = {};

        self._expectedNumRows   = undefined;

        self._wsDatasource      = undefined;
        self._wsAuthenticate    = undefined;
        self._socket            = undefined;

        self._cancelRequestsCmd = undefined;
        self._cancelRequestsAck = undefined;
        self._onSocketClose     = undefined;
        self._waitForCancelRequestsAck = false;
        self._shouldAttemptReconnect = false;

        self._ajaxDatasource   = undefined;
        self._ajaxAuthenticate = undefined;
        self._ajaxRequests = [];
        self._ajaxRequestCounter = 0;

        self._shouldClearDataset = false;

        self._rowsPerPage = 10;
        self._currentPage = undefined;

        self._onlyValidForNumbersRegex = /[^0-9.e\-]/g;

        self._hasChildElements = false;

        self._isFinished = false;

        self._replyQueue = [];

        return self;
    };

    DWH.prototype.handleMessage = function (data) {
        var self = this, reply = {};

        if (typeof(data) === "undefined") return;

        if (data.cmd === "initialize") {
            return self.initialize(data);
        } else if (data.cmd === "postMessage") {
            self._sendThroughWebSocket(data);
        } else {
            if (data.cmd in self) {
                reply = self[data.cmd](data);
            } else {
                reply.error = "Unrecognized DataWorker command: " + data.cmd;
            }
        }

        if (reply) self._postMessage(reply);
    };

    if (typeof(window) === "undefined") {
        // Running in WebWorker.

        DWH.prototype._postMessage = function _postMessage(reply, finish) {
            if (!this._isFinished) {
                globalWorker.postMessage(reply);
                if (finish) this._finish();
            }
        };

        globalWorker.onmessage = function (e) {
            if (!helper) {
                helper = new DWH();
            }

            helper.handleMessage(e.data);
        };
    } else {
        // Running in main thread.

        // Called by DWH; DataWorker uses this to receive replies.
        DWH.prototype._postMessage = function (reply, finish) {
            var self = this;

            self._replyQueue.push([ reply, finish ]);

            setTimeout(function () {
                var args   = self._replyQueue.shift(),
                    reply  = args[0],
                    finish = args[1];

                if (!self._isFinished) {
                    self.onmessage({ data: reply });
                    if (finish) self._finish();
                }
            });
        };

        // Called by DataWorker; DWH uses this to receive commands.
        DWH.prototype.postMessage = function () {
            var self = this,
                args = arguments;

            setTimeout(function () {
                self.handleMessage.apply(self, args);
            });
        };

        DWH.prototype.terminate = function () {
            this.onmessage = null;
        };

        window.DataWorkerHelper = DWH;
    }

    DWH.prototype.finish = function () {
        var self = this;

        if (self._socket && self._socket.readyState !== 0 && self._socket.readyState !== 3) {
            if (self._onSocketClose !== undefined) self._socket.send(self._onSocketClose);

            self._socket.onclose = function () {};
            self._socket.close();
        }

        self._postMessage({}, true);
    };

    DWH.prototype._finish = function () {
        var self = this;

        self._isFinished = true;
        helper = null;
    };

    /* Initializations */

    function stringify(o) { return typeof(o) === "string" ? o : JSON.stringify(o); }

    DWH.prototype.initialize = function (data) {
        var self       = this,
            datasource = typeof(data.datasource) === "string" ?
                { source: data.datasource } : data.datasource;

        self._wsDatasource = self._socket = self._ajaxDatasource = undefined;

        try {
            if (typeof(datasource) === "undefined") {
                self._columns = self._prepareColumns(data.columns);
                self._rows    = self._prepareRows(data.rows);
                self._postMessage({ connected: true });
            } else if (/^(?:https?|file):\/\//.test(datasource.source)) {
                self._ajaxDatasource   = datasource.source;
                self._ajaxAuthenticate = datasource.authenticate || data.authenticate;

                self._isLocalAjax = /^file/.test(datasource.source);

                self._verifyAjaxDatasource(data);
            } else if (/^wss?:\/\//.test(datasource.source)) {
                self._wsDatasource      = datasource.source;
                self._wsAuthenticate    = stringify(
                    datasource.authenticate || data.authenticate
                );
                self._cancelRequestsCmd = stringify(datasource.cancelRequestsCmd);
                self._cancelRequestsAck = stringify(datasource.cancelRequestsAck);

                self._shouldAttemptReconnect = data.shouldAttemptReconnect;

                self._initializeWebsocketConnection(data);
            } else {
                throw new Error("Could not initialize DataWorker; unrecognized datasource.");
            }
        } catch (error) {
            var message     = error.message,
                stack       = error.stack,
                unsupported = self._wsDatasource && !self._socket;

            if (stack) message += "\n\n" + stack;

            self._postMessage({ connected: false, error: message, unsupported: unsupported });
        }

        return self;
    };

    DWH.prototype._verifyAjaxDatasource = function (data) {
        var self = this, xmlHttp = new XMLHttpRequest();

        xmlHttp.onreadystatechange = function () {
            if (this.readyState === 4) {
                if (self._isLocalAjax ? this.response : this.status === 200) {
                    self._postMessage({ connected: true });
                    if (typeof(data.request) !== "undefined") {
                        self.requestDataset(data);
                    }
                } else {
                    self._postMessage({ connected: false, unsupported: this.status === 0 });
                }
            }
        };

        xmlHttp.open("HEAD", self._ajaxDatasource);
        xmlHttp.send();

        return true;
    };

    DWH.prototype._getRequestParams = function (params) {
        var queryString = "";

        try {
            if (params) {
                if (typeof params === "string") {
                    params = JSON.parse(params);
                }

                for (var key in params) {
                    queryString += key + "=" + encodeURIComponent(params[key]) + "&";
                }
            }
        } catch (e) {
            if (typeof params === "string") {
                queryString += params;
            }
        }

        return queryString;
    };

    DWH.prototype._processInput = function (data, allDataReceived) {
        var self = this, msg;

        try {
            msg = typeof(data) === "string" ? JSON.parse(data) : data;
        } catch (error) {
            return self._postMessage({ error: "Error " + error.message + ": " + data });
        }

        if (msg.error) {
            return self._postMessage({ error : msg.error });
        }

        if (msg.trigger) self._postMessage({ triggerMsg : msg.msg });

        if (msg.columns) {
            if (self._shouldClearDataset) self.clearDataset();

            var error = self._checkColumnsForAppend(msg.columns);
            if (error) return self._postMessage({ error: error });
        }

        if (msg.expectedNumRows === "INFINITE") {
            self._expectedNumRows = msg.expectedNumRows;
        } else if (msg.expectedNumRows !== undefined) {
            if (self._expectedNumRows === undefined) self._expectedNumRows = 0;
            self._expectedNumRows += parseInt(msg.expectedNumRows);
        }

        if (msg.rows) {
            var preparedRows = self._prepareRows(msg.rows);

            _append(preparedRows, self._rows);

            if (self._partitionedBy.length > 0) {
                self._insertIntoPartitionedRows(preparedRows);
            }

            if (self._expectedNumRows !== "INFINITE" && self._rows.length === self._expectedNumRows) {
                self._postMessage({ allRowsReceived : true });
            } else {
                self._postMessage({ rowsReceived : msg.rows.length });
            }
        }

        if (
            self._columns !== undefined &&
            (msg.expectedNumRows !== undefined || (msg.rows && msg.columns))
        ) {
            self._postMessage({ columnsReceived: true });
        }

        if (msg.summaryRows) {
            _append(self._prepareRows(msg.summaryRows), self._summaryRows);
        }

        if (
            parseInt(msg.expectedNumRows) === 0 ||
            (self._expectedNumRows === undefined && allDataReceived)
        ) {
            self._postMessage({ allRowsReceived : true });
        }
    };

    DWH.prototype._ajax = function (request) {
        var self = this,
            xmlHttp = new XMLHttpRequest(),
            url = self._ajaxDatasource + (/^\?/.test(request) ? "" : "?"),
            streamIdx = 0,
            requestCount = self._ajaxRequestCounter;

        self._ajaxRequests.push(xmlHttp);

        url += self._getRequestParams(request);
        url += self._getRequestParams(self._ajaxAuthenticate);

        xmlHttp.onreadystatechange = function () {
            if (
                requestCount === self._ajaxRequestCounter &&
                xmlHttp.readyState > 2                    &&
                (self._isLocalAjax ? xmlHttp.response : xmlHttp.status === 200)
            ) {
                var lines = xmlHttp.responseText.substr(streamIdx).split(/([\r\n]+)/);

                if (lines[lines.length - 1] && xmlHttp.readyState === 3) lines.pop();

                lines.forEach(function (line, idx) {
                    streamIdx += line.length;
                    if (idx % 2) return;
                    self._processInput(line || {}, xmlHttp.readyState === 4);
                });
            }
        };

        xmlHttp.open("GET", url, true);
        xmlHttp.send();
    };

    DWH.prototype._initializeWebsocketConnection = function (data) {
        var self = this;

        if (!self._wsDatasource) return false;

        self._socket = new WebSocket(self._wsDatasource);

        self._socket.onopen  = function () {
            if (self._wsAuthenticate) {
                self._socket.send(self._wsAuthenticate);
            }

            if (typeof(data.request) !== "undefined") {
                self._socket.send(data.request);
            }

            self._postMessage({ connected: true });
        };
        self._socket.onclose = function (e) {
            if (
                self._shouldAttemptReconnect &&
                e.code !== 1000 && e.code !== 1001
            ) {
                self._initializeWebsocketConnection(data);
            }
        };
        self._socket.onerror = function (error) {
            if (error.target.readyState === 0 || error.target.readyState === 3) {
                self._socket = null;
                self._postMessage({ connected: false });
            } else {
                self._postMessage({
                    error : "Error: Problem with connection to datasource."
                });
            }
        };

        self._socket.onmessage = function (msg) {
            if (self._waitForCancelRequestsAck && (msg.data === self._cancelRequestsAck)) {
                self._waitForCancelRequestsAck = false;
                self._postMessage({});
                return;
            }

            self._processInput(msg.data);
        };

        self._onSocketClose = data.onClose;

        return true;
    };

    /* Private Dataset Operations/Helper Functions */

    DWH.prototype._prepareColumns = function (rawColumns) {
        var preparedColumns = {};

        rawColumns.forEach(function (column, i) {
            if (typeof(column) === "string") {
                column = { name: column };
            }

            column.sortType  = column.sortType || "alpha";
            column.aggType   = column.aggType  || "max";
            column.title     = ("title" in column) ? column.title : column.name;
            column.index     = i;
            column.isVisible = ("isVisible" in column) ? column.isVisible : true;

            preparedColumns[column.name] = column;
        });

        return preparedColumns;
    };

    DWH.prototype._prepareRows = function (rawRows) {
        return rawRows.map(function (row) {
            if (!(row instanceof Array)) return row;

            return {
                row       : row,
                isVisible : true
            };
        });
    };

    DWH.prototype._getVisibleColumns = function () {
        var self = this,
            visibleColumns = {}, index = 0;

        Object.keys(self._columns).sort(function (a, b) {
            return self._columns[a].index - self._columns[b].index;
        }).forEach(function (columnName) {
            if (self._columns[columnName].isVisible) {
                visibleColumns[columnName] = JSON.parse(
                    JSON.stringify(self._columns[columnName])
                );

                visibleColumns[columnName].index = index++;
            }
        });

        return visibleColumns;
    };

    DWH.prototype._getVisibleRows = function (
        requestedColumns, requestedRows, allRows, getDistinct
    ) {
        var self = this,
            visibleColumnIdxs = [], visibleRows = [], distinctHash = {};

        if (!(requestedColumns || []).length) {
            requestedColumns = undefined;
        }
        requestedRows = requestedRows || self._rows;

        if (self._hasChildElements) {
            var rowsWithChildren = [];
            requestedRows.forEach(function (row) {
                rowsWithChildren.push(row);
                if (row.hasChildren) {
                    _append(row.children, rowsWithChildren);
                }
            });

            requestedRows = rowsWithChildren;
        }

        (requestedColumns || Object.keys(self._columns)).forEach(function (columnName) {
            var column = self._columns[columnName];

            if (column && (requestedColumns || column.isVisible)) {
                visibleColumnIdxs.push(column.index);
            }
        });

        requestedRows.forEach(function (row) {
            if (row.isVisible || allRows) {
                var newRow = visibleColumnIdxs.map(function (idx) {
                    return self._getCellDisplayValueByIndex(row, idx);
                });

                newRow.parentRow = row.parentRow;

                if (getDistinct) {
                    var seenAll = true, hash = distinctHash;

                    newRow.forEach(function (value) {
                        if (!(seenAll = seenAll && hash[value])) {
                            hash[value] = {};
                        }

                        hash = hash[value];
                    });

                    if (seenAll) return;
                }

                visibleRows.push(newRow);
            }
        });

        return visibleRows;
    };

    DWH.prototype._insertIntoPartitionedRows = function (preparedRows) {
        var self = this;

        self._hashRowsByKeyColumns(
            self._partitionedBy,
            preparedRows,
            self._partitionedRows,
            true
        );
    };

    DWH.prototype._alphaSort = function (a, b) {
        a = (a || "").toLowerCase();
        b = (b || "").toLowerCase();

        if (a < b) return -1;
        if (a > b) return  1;
        return 0;
    };

    DWH.prototype._localeAlphaSort = function (a, b) {
        a = (a || "").toLowerCase();
        b = (b || "").toLowerCase();

        return a.localeCompare(b);
    };

    DWH.prototype._numSort = function (a, b) {
        var self = this;

        a += "";
        a = a.replace(self._onlyValidForNumbersRegex, "");
        if (self._decimalMarkCharacter) a = a.replace(self._decimalMarkCharacter, ".");
        a = parseFloat(a);

        b += "";
        b = b.replace(self._onlyValidForNumbersRegex, "");
        if (self._decimalMarkCharacter) b = b.replace(self._decimalMarkCharacter, ".");
        b = parseFloat(b);

        if (!isNaN(a) && !isNaN(b))
            return a - b;
        else if (isNaN(a) && isNaN(b))
            return 0;
        else
            return isNaN(a) ? -1 : 1;
    };

    DWH.prototype._scanRows = function (data) {
        var self = this,
            filters = data.filters,
            numberCols = [],
            allIndices = [];

        Object.keys(self._columns).forEach(function (name) {
            allIndices.push(self._columns[name].index);
            numberCols[self._columns[name].index] = self._columns[name].sortType === "num";
        });

        if (typeof filters[0] === "string" || filters[0] instanceof RegExp) {
            filters = [ {
                columns: filters[1] instanceof Array ? filters[1] : filters.slice(1),
                regex: new RegExp(filters[0])
            } ];
        }

        filters = filters.map(function (filter) {
            if (filter.column) filter.columns = [ filter.column ];
            if (typeof filter.columns === "string") filter.columns = [ filter.columns ];
            if (filter.columns && filter.columns.length) {
                filter.indices = filter.columns.reduce(function (indices, columnName) {
                    var column = self._columns[columnName];
                    if (column) indices.push(column.index);

                    return indices;
                }, []);
            } else {
                filter.indices = allIndices;
            }

            filter.tests = [ "eq", "ne", "gte", "gt", "lte", "lt", "regex", "!regex" ]
                .filter(function (name) { return name in filter; });

            [ "regex", "!regex" ].forEach(function (type) {
                if (filter[type]) filter[type] = new RegExp(filter[type]);
            });
            
            if (filter.accentInsensitive) {
                filter.tests.forEach(function (testName) {
                    var testCase = filter[testName],
                        isRegex  = !!testName.match(/regex/i);
                    if (typeof testCase === "string" || isRegex) { 
                        filter[testName] = self._stripAccentMarks(testCase.toString());

                        if (isRegex) {
                            var match = filter[testName].match(new RegExp('^/(.*?)/([gimy]*)$'));
                            filter[testName] = new RegExp(match[1], match[2]);
                        }
                    }
                }); 
            }

            return filter;
        }).filter(function (filter) { return filter.indices.length; });

        return self._rows.reduce(function (results, row) {
            if (!row.isVisible && !data.allRows) return results;

            var validRow = filters.every(function (filter) {
                return filter.indices[filter.matchAll ? "every" : "some"](function (index) {
                    return filter.tests.every(function (test) {
                        return self._testCell(
                            self._getCellRawValueByIndex(row, index),
                            filter,
                            numberCols[index],
                            test
                        );
                    });
                });
            });

            if (validRow) {
                if (data.setVisibility) {
                    row.isVisible = true;
                } else {
                    results.push(row);
                }
            } else if (data.setVisibility) {
                row.isVisible = false;
            }

            return results;
        }, []);
    };

    DWH.prototype._testCell = function (cell, filter, isNum, testName) {
        var self = this;

        if (isNum) { 
            cell = parseFloat(cell);
        } else if (filter.accentInsensitive && typeof cell === "string") {
            cell = self._stripAccentMarks(cell);
        }

        switch (testName) {
            case "regex":  return filter.regex.test(cell);
            case "!regex": return !filter["!regex"].test(cell);
            case "eq":     return cell == filter.eq;
            case "ne":     return cell != filter.ne;
            case "gte":    return cell >= filter.gte;
            case "gt":     return cell >  filter.gt;
            case "lte":    return cell <= filter.lte;
            case "lt":     return cell <  filter.lt;
        }
    };

    DWH.prototype._stripAccentMarks = function (str) {
       var accentMarks = [
            /[\xC0-\xC5]/g, /[\xE0-\xE5]/g,  // A, a
            /[\xC8-\xCB]/g, /[\xE8-\xEB]/g,  // E, e
            /[\xCC-\xCF]/g, /[\xEC-\xEF]/g,  // I, i
            /[\xD2-\xD8]/g, /[\xF2-\xF8]/g,  // O, o
            /[\xD9-\xDC]/g, /[\xF9-\xFC]/g,  // U, u
            /[\xDD\u0178]/g, /[\xFD\xFF]/g,  // Y, y
            /[\xD1]/g, /[\xF1]/g,            // N, n
            /[\xC7]/g, /[\xE7]/g,            // C, c
            /[\u0160]/g, /[\u0161]/g         // S, s
       ];
       var regChars = ['A','a','E','e','I','i','O','o','U','u','Y','y','N','n','C','c','S','s'];

       accentMarks.forEach(function (accentMark, index) {
           str = str.replace(accentMark, regChars[index]);
       });

       return str;
    };

    DWH.prototype._extractColumnNamesInOrder = function (columns) {
        var self = this;

        return Object.keys(columns).sort(function (a, b) {
            return self._numSort(columns[a].index, columns[b].index);
        });
    };

    DWH.prototype._checkColumnsForAppend = function (newColumns) {
        var self = this, i, errorMsg;

        if (newColumns instanceof Array) {
            newColumns = self._prepareColumns(newColumns);
        }

        if (Object.keys(self._columns).length === 0) {
            self._columns = newColumns;
            return;
        }

        errorMsg = function () {
            var originalColumns = self._extractColumnNamesInOrder(self._columns) .join(", ");
            var appendColumns   = self._extractColumnNamesInOrder(newColumns) .join(", ");

            return "Cannot append dataset (columns do not match):\n\t" +
                originalColumns + "\n\t\tVS\n\t" + appendColumns;
        };

        if (Object.keys(newColumns).length === Object.keys(self._columns).length) {
            var columnNames = Object.keys(newColumns);

            for (i = 0; i < columnNames.length; i++) {
                var name = columnNames[i];

                if (typeof(self._columns[name]) === "undefined") {
                    return errorMsg();
                } else if (newColumns[name].index !== self._columns[name].index) {
                    return errorMsg();
                }
            }
        } else {
            return errorMsg();
        }
    };

    DWH.prototype._hashRowsByKeyColumns = function (keyColumns, myRows, hash, preparedRows) {
        var self = this,
            keyIndexes = keyColumns.map(function (columnName) {
                return self._columns[columnName].index;
            });

        myRows.forEach(function (record) {
            var key = keyIndexes.map(function (i) { return self._getCellRawValueByIndex(record, i); }).join("|");

            if (key in hash) {
                hash[key].push(preparedRows ? record : record.row);
            } else {
                hash[key] = [ preparedRows ? record : record.row ];
            }
        });

        return hash;
    };

    DWH.prototype._hashDatasetByKeyColumns = function (data, preparedRows) {
        var self = this,
            keyColumns = data.keyColumns,
            keyIndexes, hash = {}, errors = [], reply = {};

        keyColumns = keyColumns instanceof Array ? keyColumns : [ keyColumns ];
        keyColumns.forEach(function (column) {
            if (!(column in self._columns)) {
                errors.push("Column \"" + column + "\" not in dataset.");
            }
        });

        self._hashRowsByKeyColumns(keyColumns, self._rows, hash, preparedRows);

        reply.hash = hash;
        if (errors.length) reply.error = errors.join("\n");

        return reply;
    };

    DWH.prototype._getLastPage = function (data) {
        var self = this,
            visibleRows = self._getVisibleRows(data.columnNames);

        return {
            visibleRows: visibleRows,
            lastPage: (Math.ceil(visibleRows.length / self._rowsPerPage) - 1)
        };
    };

    DWH.prototype._hideShowColumns = function (data, isVisible) {
        var self = this, regex;

        if ("columnNames" in data) {
            data.columnNames.forEach(function (column) {
                if (column in self._columns) {
                    self._columns[column].isVisible = isVisible;
                }
            });
        } else if ("columnNameRegex" in data) {
            regex = new RegExp(data.columnNameRegex);
            Object.keys(self._columns).forEach(function (column) {
                if (regex.test(column)) {
                    self._columns[column].isVisible = isVisible;
                }
            });
        } else if ("property" in data) {
            Object.keys(self._columns).forEach(function (columnName) {
                var column = self._columns[columnName];

                if (column[data.property.property] === data.property.value) {
                    column.isVisible = isVisible;
                }
            });
        }
    };

    DWH.prototype._getCellValue = function (record, index, type) {
        var self = this, value = record.row[index];

        if (typeof(value) === "object" && value !== null) {
            if (value[type] !== undefined) {
                return value[type];
            } else {
                throw new Error("Unrecognized cell value format: " + value);
            }
        } else {
            return value;
        }
    };

    DWH.prototype._getCellDisplayValueByIndex = function (row, index) {
        var self = this;

        return self._getCellValue(row, index, "display");
    };

    DWH.prototype._getCellRawValueByIndex = function (row, index) {
        var self = this;

        return self._getCellValue(row, index, "raw");
    };

    DWH.prototype._getCellRawValueByColumn = function (row, columnName) {
        var self = this;

        return self._getCellRawValueByIndex(row, self._columns[columnName].index);
    };

    /* Public Dataset Operations */

    DWH.prototype.sort = function (data) {
        var self = this,
            sortOn = data.sortOn,
            rowsToSort = data.rows || self._rows;

        function _compareRows(a, b) {
            var i, sortColumn, columnName, reverse, sortType, sortResult, valA, valB;

            for (i = 0; i < sortOn.length; i++) {
                sortColumn = sortOn[i].match(/(-?)(\w+)/);
                columnName = sortColumn[2];
                reverse     = !!sortColumn[1];
                sortType   = self._columns[columnName].sortType;

                if (reverse) {
                    valB = self._getCellRawValueByColumn(a, columnName);
                    valA = self._getCellRawValueByColumn(b, columnName);
                } else {
                    valA = self._getCellRawValueByColumn(a, columnName);
                    valB = self._getCellRawValueByColumn(b, columnName);
                }

                if (sortType === "alpha") {
                    sortResult = self._alphaSort(valA, valB);
                } else if (sortType === "localeAlpha") {
                    sortResult = self._localeAlphaSort(valA, valB);
                } else if (sortType === "num") {
                    sortResult = self._numSort(valA, valB);
                } else {
                    throw new Error("Unknown sort type.");
                }

                if (sortResult !== 0) {
                    return sortResult;
                }
            }

            return 0;
        }

        rowsToSort.sort(_compareRows);

        if (self._hasChildElements) {
            rowsToSort.forEach(function (row) {
                if (row.hasChildren) {
                    row.children.sort(_compareRows);
                }
            });
        }

        return {};
    };

    DWH.prototype.setDecimalMarkCharacter = function (data) {
        var self = this;

        self._onlyValidForNumbersRegex = new RegExp(
            "[^0-9e\\-" + (self._decimalMarkCharacter = data.decimalMarkCharacter) + "]", "g"
        );

        return {};
    };

    DWH.prototype.search = function (data) {
        var self = this,
            results = self._scanRows({
                setVisibility: false,
                filters: data.filters,
                allRows: data.allRows
            });

        if (typeof data.sortOn        === "string") data.sortOn        = [ data.sortOn        ];
        if (typeof data.columns       === "string") data.columns       = [ data.columns       ];
        if (typeof data.returnColumns === "string") data.returnColumns = [ data.returnColumns ];

        if (data.sortOn) self.sort({ rows: results, sortOn: data.sortOn });

        results = self._getVisibleRows(
            data.returnColumns || data.columns,
            results,
            true,
            data.getDistinct
        );

        if (data.limit > 0) results = results.slice(data.fromRow, data.fromRow + data.limit);

        return { rows : results };
    };

    DWH.prototype.applyFilter = function (data) {
        var self = this;

        data.setVisibility = true;
        self._scanRows(data);

        return {};
    };

    DWH.prototype.clearFilters = function () {
        var self = this;

        self._rows.forEach(function (row) {
            row.isVisible = true;
        });

        return {};
    };

    DWH.prototype.filter = function (data) {
        var self = this;

        self._rows = self._scanRows(data);

        return {};
    };

    DWH.prototype.applyLimit = function (data) {
        var self = this,
            numRows = data.numRows, i = 0;

        self._rows.forEach(function (row) {
            if (i++ < numRows) {
                row.isVisible = true;
            } else {
                row.isVisible = false;
            }
        });

        return {};
    };

    DWH.prototype.limit = function (data) {
        var self = this, numRows = data.numRows;

        self._rows = self._rows.slice(0, numRows);

        return {};
    };

    DWH.prototype.removeColumns = function (data) {
        var self = this,
            columnsToRemove = data.columnsToRemove,
            i = 0, columnsToKeep = {};

        Object.keys(self._columns).forEach(function (columnName, i) {
            if (columnsToRemove.indexOf(columnName) === -1) {
                var column = self._columns[columnName];
                column.index = i;

                columnsToKeep[columnName] = column;
            }
        });

        self._rows.forEach(function (record) {
            var filteredRow = [];

            Object.keys(columnsToKeep).forEach(function (columnName) {
                filteredRow[columnsToKeep[columnName].index] =
                    record.row[self._columns[columnName].index];
            });

            record.row = filteredRow;
        });

        self._columns = columnsToKeep;

        return {};
    };

    DWH.prototype.prependColumnNames = function (data) {
        var self = this,
            prepend = data.prepend,
            newColumns = {};

        Object.keys(self._columns).forEach(function (columnName) {
            newColumns[prepend + columnName] = self._columns[columnName];
        });

        self._columns = newColumns;

        return {};
    };

    DWH.prototype.alterColumnName = function (data) {
        var self = this,
            oldName = data.oldName, newName = data.newName,
            error;

        Object.keys(self._columns).forEach(function (columnName) {
            if (columnName !== oldName && columnName === newName) {
                error = "Column " + newName + " already exists in the dataset.";
            }
        });

        self._columns[newName] = self._columns[oldName];
        delete self._columns[oldName];

        return typeof(error) === "undefined" ? {} : { error : error };
    };

    DWH.prototype.alterColumnSortType = function (data) {
        var self = this,
            column = data.column, sortType = data.sortType;

        self._columns[column].sortType = sortType;

        return {};
    };

    DWH.prototype.alterColumnAggType = function (data) {
        var self = this,
            column = data.column, aggType = data.aggType;

        self._columns[column].aggType = aggType;

        return {};
    };

    DWH.prototype.alterColumnTitle = function (data) {
        var self = this,
            column = data.column, title = data.title;

        self._columns[column].title = title;

        return {};
    };

    DWH.prototype.append = function (data) {
        var self = this;

        if (data.newColumns) {
            var error = self._checkColumnsForAppend(data.newColumns);
            if (error) return { error: error };
        }

        self._rows.splice.apply(self._rows,
            [ "index" in data ? data.index : self._rows.length, 0 ].concat(
                data.preparedRows || self._prepareRows(data.newRows)
            )
        );

        return {};
    };

    DWH.prototype.hash = function (data) {
        var self = this;
        return self._hashDatasetByKeyColumns(data);
    };

    DWH.prototype.join = function (data) {
        var self = this,
            lHash = self._hashDatasetByKeyColumns(data), rHash = data.rHash,
            joinType = data.joinType, fColumns = data.fColumns;

        if ("error" in lHash) {
            return { error : lHash.error };
        } else {
            lHash = lHash.hash;
        }

        var joinedDataset = [],
            pHash = joinType === "right" ? rHash : lHash,
            fHash = joinType === "right" ? lHash : rHash,
            emptyOuterRow = fHash[Object.keys(fHash)[0]][0].map(function () {
                return "";
            }),
            originalNumColumns = Object.keys(self._columns).length;

        joinType = typeof(joinType) === "undefined" ? "inner" : joinType;

        Object.keys(pHash).forEach(function (keyField) {
            if (keyField in fHash) {
                pHash[keyField].forEach(function (pRow) {
                    fHash[keyField].forEach(function (fRow) {
                        joinedDataset.push(
                            joinType === "right" ? fRow.concat(pRow)
                                                 : pRow.concat(fRow)
                        );
                    });
                });
            } else if (joinType !== "inner") {
                pHash[keyField].forEach(function (pRow) {
                    joinedDataset.push(
                        joinType === "right" ? emptyOuterRow.concat(pRow)
                                             : pRow.concat(emptyOuterRow)
                    );
                });
            }
        });

        Object.keys(fColumns).forEach(function (columnName) {
            var column = fColumns[columnName];
            column.index += originalNumColumns;

            self._columns[columnName] = column;
        });

        self._rows = self._prepareRows(joinedDataset);

        return {};
    };

    DWH.prototype.group = function (data) {
        var self = this,
            hashedDataset = self._hashDatasetByKeyColumns(data), groupBy = data.keyColumns,
            groupedDataset = [], errors = [];

        if ("error" in hashedDataset) {
            return { "error" : hashedDataset.error };
        } else {
            hashedDataset = hashedDataset.hash;
        }

        Object.keys(hashedDataset).forEach(function (key) {
            var groupedRow = [];

            hashedDataset[key].forEach(function (row) {
                Object.keys(self._columns).forEach(function (columnName) {
                    var aggType = self._columns[columnName].aggType,
                        index = self._columns[columnName].index,
                        isKeyColumn = groupBy.indexOf(columnName) !== -1;

                    if (index in groupedRow && !isKeyColumn) {
                        if (aggType === "sum") {
                            groupedRow[index] += row[index];
                        } else if (aggType === "max") {
                            groupedRow[index] = ( groupedRow[index] < row[index] ) ?
                                row[index] : groupedRow[index];
                        } else if (aggType === "min") {
                            groupedRow[index] = ( groupedRow[index] > row[index] ) ?
                                row[index] : groupedRow[index];
                        } else {
                            errors.push(
                                "Unrecognized aggType for columm \"" + columnName + "\"."
                            );
                        }
                    } else {
                        groupedRow[index] = row[index];
                    }
                });
            });

            groupedDataset.push(groupedRow);
        });

        self._rows = self._prepareRows(groupedDataset);

        return errors.length > 0 ? { error : errors.join("\n") } : {};
    };

    DWH.prototype.partition = function (data) {
        var self = this,
            hashedDataset = self._hashDatasetByKeyColumns(data, true);

        if ("error" in hashedDataset) {
            return { "error" : hashedDataset.error };
        } else {
            hashedDataset = hashedDataset.hash;
        }

        self._partitionedBy = data.keyColumns;

        var columnsRow = Object.keys(self._columns).map(function (columnName) {
            return self._columns[columnName];
        }).sort(function (a, b) {
            return self._numSort(a.index, b.index);
        });

        Object.keys(hashedDataset).forEach(function (key) {
            var dataset = hashedDataset[key];
            self._partitionedRows[key] = dataset;
        });

        return {};
    };

    DWH.prototype.getPartitionKeys = function (data) {
        var self = this,
            keys =  Object.keys(self._partitionedRows).map(function (key) {
                return key.split("|");
            });

        return { keys : keys };
    };

    DWH.prototype.getPartitioned = function (data) {
        var self = this, rows = self._partitionedRows[data.key];
        return {
            rows: rows ? self._getVisibleRows(undefined, self._partitionedRows[data.key]) : []
        };
    };

    DWH.prototype.sortPartition = function (data) {
        var self = this;

        if (data.sortOn === "string") data.sortOn = [ data.sortOn ];
        self.sort({ rows: self._partitionedRows[data.key], sortOn: data.sortOn });

        return {};
    };

    DWH.prototype.getDataset = function (data) {
        var self = this;
        return { rows : self._getVisibleRows(data.columnNames) };
    };

    DWH.prototype.getDistinctConsecutiveRows = function (data) {
        var self = this,
            visibleRows = self._getVisibleRows([ data.columnName ]),
            distinctConsecutiveRows = [],
            currentRow = 0,
            currentValue;

        visibleRows.forEach(function (row, i) {
            if (!i || (!row.parentRow && (currentValue != row[0]))) {
                if (distinctConsecutiveRows.length) {
                    distinctConsecutiveRows[currentRow++][2] = i - 1;
                }

                currentValue = row[0];
                distinctConsecutiveRows.push([ currentValue, i, i ]);
            }
        });

        if (distinctConsecutiveRows.length) {
            distinctConsecutiveRows[currentRow++][2] = visibleRows.length - 1;
        }

        return { distinctRows : distinctConsecutiveRows };
    };

    DWH.prototype.getNumberOfRecords = function (data) {
        var self = this;
        return { numRows : self._getVisibleRows().length };
    };

    DWH.prototype.paginate = function (data) {
        var self = this;
        self._rowsPerPage = data.rowsPerPage;
        return {};
    };

    DWH.prototype.getPage = function (data) {
        var self = this,
            rowData = self._getLastPage(data),
            start, end;

        data.lastPage = rowData.lastPage;

        self.setPage(data);

        start = self._rowsPerPage * self._currentPage;
        end = start + self._rowsPerPage;

        return {
            rows        : rowData.visibleRows.slice(start, end),
            currentPage : self._currentPage + 1
        };
    };

    DWH.prototype.setPage = function (data) {
        var self = this;

        if (!("lastPage" in data)) {
            data.lastPage = self._getLastPage(data).lastPage;
        }

        if (typeof(data.pageNum) !== "undefined") {
            self._currentPage = data.pageNum - 1;
        } else if (typeof(self._currentPage) === "undefined") {
            self._currentPage = 0;
        } else if (data.incrementPage) {
            self._currentPage++;
        } else if (data.decrementPage) {
            self._currentPage--;
        }

        if (self._currentPage < 0) self._currentPage = 0;
        if (self._currentPage > data.lastPage) self._currentPage = data.lastPage;

        return { currentPage: self._currentPage + 1 };
    };

    DWH.prototype.getNumberOfPages = function (data) {
        var self = this,
            rowData = self._getLastPage(data);

        return { numberOfPages: rowData.lastPage + 1 };
    };

    DWH.prototype.hideColumns = function (data) {
        var self = this;

        self._hideShowColumns(data, false);

        return {};
    };

    DWH.prototype.showColumns = function (data) {
        var self = this;

        self._hideShowColumns(data, true);

        return {};
    };

    DWH.prototype.hideAllColumns = function (data) {
        var self = this;

        Object.keys(self._columns).forEach(function (column) {
            self._columns[column].isVisible = false;
        });

        return {};
    };

    DWH.prototype.showAllColumns = function (data) {
        var self = this;

        Object.keys(self._columns).forEach(function (column) {
            self._columns[column].isVisible = true;
        });

        return {};
    };

    DWH.prototype.getAllColumns = function (data) {
        var self = this;
        return { columns : self._columns };
    };

    DWH.prototype.getColumns = function (data) {
        var self = this;
        return { columns : self._getVisibleColumns() };
    };

    DWH.prototype.getRows = function (data) {
        var self = this,
            start = data.start, end = data.end;

        if (typeof(end) !== "undefined") {
            end = parseInt(end) + 1;
        }

        return { rows : self.getDataset(data).rows.slice(start, end) };
    };

    DWH.prototype.getHashedRows = function (data) {
        var self = this,
            columns = data.columnNames,
            rows    = self.getRows(data).rows;

        if (!columns.length) {
            columns = self.getColumns().columns;
            columns = Object.keys(columns).sort(function (a, b) {
                return columns[a].index - columns[b].index;
            });
        }

        return {
            rows: rows.map(function (row) {
                return columns.reduce(function (hash, name, index) {
                    hash[name] = row[index];
                    return hash;
                }, {});
            })
        };
    };

    DWH.prototype.getSummaryRows = function (data) {
        var self = this;
        return { summaryRows : self._getVisibleRows(data.columnNames, self._summaryRows) };
    };

    DWH.prototype.setSummaryRows = function (data) {
        var self = this;
        self._summaryRows = self._prepareRows(data.summaryRows);
        return {};
    };

    DWH.prototype.getExpectedNumRows = function (data) {
        var self = this;
        return { exNumRows : self._expectedNumRows };
    };

    DWH.prototype.clearDataset = function () {
        var self = this;

        self._columns         = {};
        self._rows            = [];
        self._summaryRows     = [];
        self._partitionedRows = [];
        self._expectedNumRows = undefined;

        self._shouldClearDataset = false;

        return {};
    };

    DWH.prototype.requestDataset = function (data) {
        var self = this;

        self._shouldClearDataset = true;
        return self.requestDatasetForAppend(data);
    };

    DWH.prototype.requestDatasetForAppend = function (data) {
        var self = this, requestMade = false;

        if (typeof(self._socket) !== "undefined") {
            self._socket.send(data.request);
            requestMade = true;
        } else if (typeof(self._ajaxDatasource) !== "undefined") {
            self._ajax(data.request);
            requestMade = true;
        }

        return requestMade ? {} : { error: "Could not request dataset; no datasource defined." };
    };

    DWH.prototype.addChildRows = function (data) {
        var self = this,
            newRows = self._prepareRows(data.rows),
            rowHash = self._hashRowsByKeyColumns([ data.joinOn ], newRows, {}, true),
            joinIdx = self._columns[data.joinOn].index;

        self._rows.forEach(function (row, i) {
            var children = rowHash[self._getCellRawValueByIndex(row, joinIdx)];

            if (children) {
                self._hasChildElements = true;
                row.hasChildren = true;
                row.children = children;
                children.forEach(function (child) {
                    child.parentRow = row;
                    child.isVisible = row.isVisible;
                });
            }
        });

        return {};
    };

    DWH.prototype.refresh = function (data) {
        var self = this;
        return { columns: self._getVisibleColumns(), rows: self._getVisibleRows() };
    };

    DWH.prototype.refreshAll = function (data) {
        var self = this, rows;

        if (data.complexValues) {
            rows = self._rows.map(function (record) { return record.row; });
        } else {
            rows = self._rows.map(function (record) {
                return record.row.map(function (cell, i) {
                    return self._getCellDisplayValueByIndex(record, i);
                });
            });
        }

        return { columns: self._columns, rows: rows };
    };

    DWH.prototype.cancelOngoingRequests = function (data) {
        var self = this;

        if (self._ajaxDatasource) {
            self._ajaxRequests.forEach(function (xhr) { xhr.abort(); });
            self._ajaxRequests = [];
            self._ajaxRequestCounter++;
        } else if (self._wsDatasource) {
            if (
                self._socket            !== undefined &&
                self._cancelRequestsCmd !== undefined
            ) {
                self._waitForCancelRequestsAck = !!self._cancelRequestsAck;
                self._socket.send(self._cancelRequestsCmd);

                if (self._waitForCancelRequestsAck) return;
            }
        }

        return {};
    };

    /* Miscellaneous */

    DWH.prototype._sendThroughWebSocket = function (data) {
        var self = this;

        if (
            typeof(self._socket) !== "undefined" &&
            self._socket.readyState !== 0        &&
            self._socket.readyState !== 3
        ) {
            self._socket.send(data.message);
        }
    };

    function _append(src, dest) {
        var srcLength = src.length,
            srcIndex  = 0,
            destIndex = dest.length;

        while (srcIndex < srcLength) {
            dest[destIndex++] = src[srcIndex++];
        }
    }
}

DataWorkerHelperCreator(this);

/*!
 * document.currentScript
 * Polyfill for `document.currentScript`.
 * Copyright (c) 2015 James M. Greene
 * Licensed MIT
 * http://jsfiddle.net/JamesMGreene/9DFc9/
 * v0.1.7
 */
(function() {
"use strict";

if (typeof window === "undefined") { return; }


var hasStackBeforeThrowing = false,
    hasStackAfterThrowing = false;
(function() {
  try {
    var err = new Error();
    hasStackBeforeThrowing = typeof err.stack === "string" && !!err.stack;
    throw err;
  }
  catch (thrownErr) {
    hasStackAfterThrowing = typeof thrownErr.stack === "string" && !!thrownErr.stack;
  }
})();


// This page's URL
var pageUrl = window.location.href;

// Live NodeList collection
var scripts = document.getElementsByTagName("script");

// Get script object based on the `src` URL
function getScriptFromUrl(url) {
  if (typeof url === "string" && url) {
    for (var i = 0, len = scripts.length; i < len; i++) {
      if (scripts[i].src === url) {
        return scripts[i];
      }
    }
  }
  return null;
}

// If there is only a single inline script on the page, return it; otherwise `null`
function getSoleInlineScript() {
  var script = null;
  for (var i = 0, len = scripts.length; i < len; i++) {
    if (!scripts[i].src) {
      if (script) {
        return null;
      }
      script = scripts[i];
    }
  }
  return script;
}

// Get the configured default value for how many layers of stack depth to ignore
function getStackDepthToSkip() {
  var depth = 0;
  if (
    typeof _currentScript !== "undefined" &&
    _currentScript &&
    typeof _currentScript.skipStackDepth === "number"
  ) {
    depth = _currentScript.skipStackDepth;
  }
  return depth;
}

// Get the currently executing script URL from an Error stack trace
function getScriptUrlFromStack(stack, skipStackDepth) {
  var url, matches, remainingStack,
      ignoreMessage = typeof skipStackDepth === "number";
  skipStackDepth = ignoreMessage ? skipStackDepth : getStackDepthToSkip();
  if (typeof stack === "string" && stack) {
    if (ignoreMessage) {
      matches = stack.match(/(data:text\/javascript(?:;[^,]+)?,.+?|(?:|blob:)(?:http[s]?|file):\/\/[\/]?.+?\/[^:\)]*?)(?::\d+)(?::\d+)?/);
    }
    else {
      matches = stack.match(/^(?:|[^:@]*@|.+\)@(?=data:text\/javascript|blob|http[s]?|file)|.+?\s+(?: at |@)(?:[^:\(]+ )*[\(]?)(data:text\/javascript(?:;[^,]+)?,.+?|(?:|blob:)(?:http[s]?|file):\/\/[\/]?.+?\/[^:\)]*?)(?::\d+)(?::\d+)?/);

      if (!(matches && matches[1])) {
        matches = stack.match(/\)@(data:text\/javascript(?:;[^,]+)?,.+?|(?:|blob:)(?:http[s]?|file):\/\/[\/]?.+?\/[^:\)]*?)(?::\d+)(?::\d+)?/);
      }
    }

    if (matches && matches[1]) {
      if (skipStackDepth > 0) {
        remainingStack = stack.slice(stack.indexOf(matches[0]) + matches[0].length);
        url = getScriptUrlFromStack(remainingStack, (skipStackDepth - 1));
      }
      else {
        url = matches[1];
      }
    }
  }
  return url;
}

// Get the currently executing `script` DOM element
function _currentScript() {
  // Yes, this IS actually possible
  if (scripts.length === 0) {
    return null;
  }

  if (scripts.length === 1) {
    return scripts[0];
  }

  if ("readyState" in scripts[0]) {
    for (var i = scripts.length; i--; ) {
      if (scripts[i].readyState === "interactive") {
        return scripts[i];
      }
    }
  }

  if (document.readyState === "loading") {
    return scripts[scripts.length - 1];
  }

  var stack,
      e = new Error();
  if (hasStackBeforeThrowing) {
    stack = e.stack;
  }
  if (!stack && hasStackAfterThrowing) {
    try {
      throw e;
    }
    catch (err) {
      // NOTE: Cannot use `err.sourceURL` or `err.fileName` as they will always be THIS script
      stack = err.stack;
    }
  }
  if (stack) {
    var url = getScriptUrlFromStack(stack);
    var script = getScriptFromUrl(url);
    if (!script && url === pageUrl) {
      script = getSoleInlineScript();
    }
    return script;
  }

  return null;
}


// Configuration
_currentScript.skipStackDepth = 1;



// Inspect the polyfill-ability of this browser
var needsPolyfill = !("currentScript" in document);
var canDefineGetter = document.__defineGetter__;
var canDefineProp = typeof Object.defineProperty === "function" &&
  (function() {
    var result;
    try {
      Object.defineProperty(document, "_xyz", {
        get: function() {
          return "blah";
        },
        configurable: true
      });
      result = document._xyz === "blah";
      delete document._xyz;
    }
    catch (e) {
      result = false;
    }
    return result;
  })();


// Add the "private" property for testing, even if the real property can be polyfilled
document._currentScript = _currentScript;

// Polyfill it!
if (needsPolyfill) {
  if (canDefineProp) {
    Object.defineProperty(document, "currentScript", {
      get: _currentScript
    });
  }
  else if (canDefineGetter) {
    document.__defineGetter__("currentScript", _currentScript);
  }
}

})();

(function() {
    "use strict";

    if (typeof window === "undefined") {
        return;
    }

    var DataWorker = window.DataWorker = function DataWorker (dataset) {
        var self = this instanceof DataWorker ? this : Object.create(DataWorker.prototype);

        self._columns      = {};
        self._rows         = [];
        self._summaryRows  = [];
        self._distinctRows = [];
        self._hash         = {};

        self._numRows = 0;
        self._expectedNumRows = 0;
        self._currentPage = undefined;
        self._numberOfPages = undefined;

        self._partitionedDatasets = {};

        self._renderFunction = function () {};

        self._actionQueue = new ActionQueue();

        self._initializeCallbacks(dataset);
        self._initializeSettings(dataset);
        self._initializeDatasources(dataset);
        self._queueNext(function () {
            self._initializeWebWorker();
        });

        return self;
    };

    DataWorker.currentScript = (document.currentScript || document._currentScript() || {}).src;

    DataWorker.ignoreProtocols = {};

    DataWorker.workerPool = new WebWorkerPool();
    DataWorker.prototype.workerPool = DataWorker.workerPool;

    DataWorker.prototype._queueNext = function (action) {
        var self = this;

        self._actionQueue.queueNext(action);

        return self;
    };

    DataWorker.prototype._finishAction = function (finishAsynchronous) {
        var self = this;

        if (finishAsynchronous) {
            self._actionQueue.finishAsynchronous();
        }

        self._actionQueue.finishAction();

        return self;
    };

    DataWorker.prototype._postMessage = function (message) {
        var self = this;

        self._actionQueue.beginAsynchronous();
        self._worker.postMessage(message);

        return self;
    };

    DataWorker.prototype._initializeCallbacks = function (dataset) {
        var self = this;

        self._onReceiveColumnsTracker = false;
        if ("onReceiveColumns" in dataset) {
            self._onReceiveColumns = function () {
                self._onReceiveColumnsTracker = false;
                dataset.onReceiveColumns.apply(this, arguments);
            };
        } else {
            self._onReceiveColumns = function () {};
        }

        self._onAllRowsReceivedTracker = false;
        if ("onAllRowsReceived" in dataset) {
            self._onAllRowsReceived = function () {
                self._onAllRowsReceivedTracker = false;
                dataset.onAllRowsReceived.apply(this, arguments);
            };
        } else {
            self._onAllRowsReceived = function () {};
        }

        self._onReceiveRows = "onReceiveRows" in dataset ? dataset.onReceiveRows : function () {};
        self._onTrigger     = "onTrigger"     in dataset ? dataset.onTrigger     : function () {};

        self._onError = "onError" in dataset ? dataset.onError : function (error) {
            if (typeof console !== "undefined" && console.error) console.error(error);
        };

        return self;
    };

    DataWorker.prototype._initializeSettings = function (dataset) {
        var self    = this,
            sources = self._workerSources = [];

        if (dataset.forceSingleThread || typeof Worker === "undefined") {
            self._isSingleThreaded = true;
            return self;
        }

        if ("workerSource" in dataset) {
            sources.push(dataset.workerSource);
        }

        if (!DataWorker.helperBlob) {
            try {
                var code = DataWorkerHelperCreator + "; DataWorkerHelperCreator(this);";

                DataWorker.helperBlob = new Blob([ code ], { type: "application/javascript" });
                DataWorker.helperBlobUrl = URL.createObjectURL(DataWorker.helperBlob);
            } catch (error) {
                DataWorker.helperBlob = error;
            }
        }

        if (DataWorker.helperBlobUrl) {
            sources.push(DataWorker.helperBlobUrl);
        }

        if ("backupWorkerSource" in dataset) {
            sources.push(dataset.backupWorkerSource);
        }

        if (DataWorker.currentScript) {
            sources.push(DataWorker.currentScript);
        }

        self._workerSources = sources.filter(function (source) {
            var ignore       = DataWorker.ignoreProtocols[source] || {},
                datasource   = [].concat(dataset.datasource || []),
                hasPotential = datasource.some(function (address) {
                    return !ignore[(/^(wss?|https?|file):/.exec(address) || [])[1]];
                });

            DataWorker.ignoreProtocols[source] = ignore;

            return hasPotential;
        });

        return self;
    };

    DataWorker.prototype._initializeDatasources = function (settings) {
        var self        = this,
            datasources = [],
            dataset     = settings.dataset || settings,
            columns, rows, request;

        if (dataset instanceof Array) {
            columns = dataset.slice(0, 1)[0];
            rows    = dataset.slice(1);
        } else if ((settings.columns instanceof Array) && (settings.rows instanceof Array)) {
            columns = settings.columns;
            rows    = settings.rows;
        } else {
            datasources = (settings.datasource instanceof Array) ?
                settings.datasource.slice(0) : [ settings.datasource ];

            request = (typeof settings.request === "string") ?
                settings.request : JSON.stringify(settings.request);
        }

        self._connectionSettings = {
            index: 0,
            datasources: datasources,
            message: {
                cmd                    : "initialize",

                columns                : columns,
                rows                   : rows,

                authenticate           : settings.authenticate,
                request                : request,
                onClose                : settings.onClose,
                shouldAttemptReconnect : settings.shouldAttemptReconnect
            }
        };

        return self;
    };

    DataWorker.prototype._initializeWebWorker = function () {
        var self = this;

            if (!(self._worker = self.workerPool.getWorker(self._workerSources))) {
                self._isSingleThreaded = true;
                self._worker = new DataWorkerHelper();
            }

            self._worker.onmessage = function (e) {
                if (!e.data) return;

                if (self._resettingWebworker) {
                    self._resettingWebworker = false;
                    self.workerPool.reclaim(self._worker);
                    self._worker.onmessage = null;
                    return self._initializeWebWorker();
                }

                if ("connected" in e.data) {
                    if (!e.data.connected) return self._connect(true, e.data);
                }

                if ("rowsReceived" in e.data) {
                    if (self._onReceiveRows) self._onReceiveRows(e.data.rowsReceived);
                    return;
                }
                if ("triggerMsg" in e.data) {
                    if (self._onTrigger) self._onTrigger(e.data.triggerMsg);
                    return;
                }
                if ("allRowsReceived" in e.data) {
                    self._onAllRowsReceivedTracker = true;
                    if (self._onAllRowsReceived) self._onAllRowsReceived();
                    return;
                }

                if ("currentPage" in e.data) {
                    self._currentPage = e.data.currentPage;
                }

                if ("numberOfPages" in e.data) {
                    self._numberOfPages = e.data.numberOfPages;
                }

                if ("error" in e.data) self._onError(e.data.error);

                if ("columns"      in e.data) self._columns = e.data.columns;
                if ("rows"         in e.data) self._rows = e.data.rows;
                if ("summaryRows"  in e.data) self._summaryRows = e.data.summaryRows;
                if ("distinctRows" in e.data) self._distinctRows = e.data.distinctRows;

                if ("hash"         in e.data) self._hash = e.data.hash;
                if ("keys"         in e.data) self._keys = e.data.keys;

                if ("numRows"      in e.data) self._numRows = e.data.numRows;
                if ("exNumRows"    in e.data) self._expectedNumRows = e.data.exNumRows;

                if ("columnsReceived" in e.data) {
                    self._onReceiveColumnsTracker = true;
                    if (self._onReceiveColumns) self._onReceiveColumns();
                    return;
                }

                self._finishAction(true);
            };

            self._connect(false);

        return self;
    };

    DataWorker.prototype._connect = function (async, data) {
        var self          = this,
            settings      = self._connectionSettings,
            error         = data && data.error,
            unsupported   = data && data.unsupported,
            hasLocalData  = settings.message.columns && settings.message.rows && true,
            hasRemoteData = settings.datasources.length > settings.index;

        if (unsupported && self._worker.source) {
            var datasource = settings.datasources[settings.index - 1],
                sourceStr  = (datasource && datasource.source) || datasource || "",
                protocol   = (/^(wss?|https?|file):/.exec(sourceStr) || [])[1];

            DataWorker.ignoreProtocols[self._worker.source][protocol] = true;
        } else if (error) {
            self._onError(error);
        }

        if (hasRemoteData) {
            settings.message.datasource = settings.datasources[settings.index++];
        } else if (!hasLocalData) {
            if (self._worker.source) {
                self._workerSources = self._workerSources.filter(function (source) {
                    return self._worker.source !== source;
                });

                settings.index = 0;

                self._resettingWebworker = true;
                self._postMessage({ cmd: "finish" });
            } else {
                self._onError("No available datasources.");
                self._finishAction(async);
            }

            return self;
        }

        self._postMessage(settings.message);

        return self;
    };

    DataWorker.prototype.finish = function (cb) {
        var self = this;

        self.cancelOngoingRequests();

        self._queueNext(function () {
            self._onReceiveColumns  = null;
            self._onReceiveRows     = null;
            self._onAllRowsReceived = null;
            self._onTrigger         = null;
            self._onError           = null;

            self._renderFunction    = null;

            self._postMessage({ cmd: "finish" });
        })._queueNext(function () {
            self.workerPool.reclaim(self._worker);
            self._actionQueue.finish();

            self._worker.onmessage = null;
            self._worker = null;

            if (cb) cb();
        });

        return self;
    };

    DataWorker.prototype.getColumns = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "getColumns" });
        })._queueNext(function () {
            callback.call(self, self._columns);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getDistinctConsecutiveRows = function () {
        var self = this,
            callback = arguments[0],
            columnName = arguments[1];

        self._queueNext(function () {
            self._postMessage({
                cmd        : "getDistinctConsecutiveRows",
                columnName : columnName
            });
        })._queueNext(function () {
            callback.call(self, self._distinctRows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype._filter = function (cmd, filters) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : cmd,
                filters : filters
            });
        });

        return self;
    };

    DataWorker.prototype.clearFilters = function () {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd : "clearFilters"
            });
        });

        return self;
    };

    DataWorker.prototype.applyFilter = function () {
        var self = this,
            filters = _getArray(arguments);

        return self._filter("applyFilter", filters);
    };

    DataWorker.prototype.filter = function () {
        var self = this,
            filters = _getArray(arguments);

        return self._filter("filter", filters);
    };

    DataWorker.prototype.applyLimit = function (numRows) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "applyLimit",
                numRows : numRows
            });
        });

        return self;
    };

    DataWorker.prototype.limit = function (numRows) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "limit",
                numRows : numRows
            });
        });

        return self;
    };

    DataWorker.prototype.sort = function () {
        var self = this, sortColumns = _getArray(arguments);

        self._queueNext(function () {
            self._postMessage({
                cmd     : "sort",
                sortOn  : sortColumns
            });
        });

        return self;
    };

    DataWorker.prototype.setDecimalMarkCharacter = function (decimalMarkCharacter) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd                  : "setDecimalMarkCharacter",
                decimalMarkCharacter : decimalMarkCharacter
            });
        });

        return self;
    };

    DataWorker.prototype.removeColumns = function () {
        var self = this,
            columnsToRemove = _getArray(arguments);

        self._queueNext(function () {
            self._postMessage({
                cmd             : "removeColumns",
                columnsToRemove : columnsToRemove
            });
        });

        return self;
    };

    DataWorker.prototype.paginate = function (rowsPerPage) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd         : "paginate",
                rowsPerPage : rowsPerPage
            });
        });

        return self;
    };

    DataWorker.prototype.getNextPage = function (callback) {
        var self        = this,
            columnNames = _getArray(arguments, 1);

        self._queueNext(function () {
            self._getPage(undefined, true, false, columnNames);
        })._queueNext(function() {
            callback.call(self, self._rows, self._currentPage);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getPreviousPage = function (callback) {
        var self        = this,
            columnNames = _getArray(arguments, 1);

        self._queueNext(function () {
            self._getPage(undefined, false, true, columnNames);
        })._queueNext(function () {
            callback.call(self, self._rows, self._currentPage);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getNumberOfPages = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd : "getNumberOfPages"
            });
        })._queueNext(function () {
            callback.call(self, self._numberOfPages);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype._getPage = function (pageNum, incrementPage, decrementPage, columnNames) {
        var self = this;

        self._postMessage({
            cmd           : "getPage",
            pageNum       : pageNum,
            columnNames   : columnNames,
            incrementPage : incrementPage,
            decrementPage : decrementPage
        });

        return self;
    };

    DataWorker.prototype.getPage = function (callback, pageNum) {
        var self        = this,
            columnNames = _getArray(arguments, 2);

        self._queueNext(function () {
            self._getPage(pageNum, undefined, undefined, columnNames);
        })._queueNext(function () {
            callback.call(self, self._rows, self._currentPage);
            self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.setPage = function (pageNum) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "setPage",
                pageNum : pageNum
            });
        });

        return self;
    };

    DataWorker.prototype.getRows = function (callback, start, end) {
        var self        = this,
            columnNames = _getArray(arguments, 3);

        self._queueNext(function () {
            self._postMessage({
                cmd         : "getRows",
                start       : start,
                end         : end,
                columnNames : columnNames
            });
        })._queueNext(function () {
            callback.call(self, self._rows);
            self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getHashedRows = function (callback, start, end) {
        var self        = this,
            columnNames = _getArray(arguments, 3);

        self._queueNext(function () {
            self._postMessage({
                cmd         : "getHashedRows",
                start       : start,
                end         : end,
                columnNames : columnNames
            });
        })._queueNext(function () {
            callback.call(self, self._rows);
            self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getColumnsAndRecords = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "refresh" });
        })._queueNext(function () {
            callback.call(self, self._columns, self._rows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getAllColumnsAndAllRecords = function (callback, complexValues) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd: "refreshAll", complexValues: complexValues });
        })._queueNext(function () {
            callback.call(self, self._columns, self._rows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.append = function (data) {
        var self = this;

        self._queueNext(function () {
            if (data instanceof DataWorker) {
                data.getAllColumnsAndAllRecords(function (newColumns, newRows) {
                    self._postMessage({
                        cmd        : "append",
                        newColumns : newColumns,
                        newRows    : newRows
                    });
                }, true);
            } else {
                self._postMessage({
                    cmd         : "append",
                    newColumns : data.slice(0, 1)[0],
                    newRows    : data.slice(1)
                });
            }
        });

        return self;
    };

    DataWorker.prototype.join = function (fdata, pk, fk, joinType) {
        var self = this, args = Array.prototype.slice.call(arguments);
        var fHash, fColumns;

        self._queueNext(function () {
            if (!pk.length || !fk.length) {
                self._onError("No join key(s) provided.");
            }
            if (pk.length != fk.length) {
                self._onError("Odd number of join keys.");
            }
            if (
                typeof(joinType) !== "undefined" &&
                !(joinType === "left" || joinType === "right" || joinType === "inner")
            ) {
                self._onError("Unknown join type.");
            }

            return self._finishAction();
        });

        self._queueNext(function () {
            self._postMessage({ cmd : "getAllColumns" });
        })._queueNext(function () {
            fdata.getAllColumns(function (columns) {
                Object.keys(columns).forEach(function (columnName) {
                    if (columnName in self._columns) {
                        self._onError("Column names overlap.");
                    }
                });

                fColumns = columns;
                return self._finishAction();
            });
        })._queueNext(function () {
            fdata.getHashOfDatasetByKeyColumns.call(fdata, function (hash) {
                fHash = hash;
                return self._finishAction();
            }, fk);
        })._queueNext(function () {
            self._postMessage({
                cmd        : "join",
                keyColumns : pk,
                rHash      : fHash,
                joinType   : joinType,
                fColumns   : fColumns
            });
        });

        return self;
    };

    DataWorker.prototype.getHashOfDatasetByKeyColumns = function (callback, keyColumns) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd        : "hash",
                keyColumns : keyColumns
            });
        })._queueNext(function () {
            callback.call(self, self._hash);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.prependColumnNames = function (prepend) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "prependColumnNames",
                prepend : prepend
            });
        });

        return self;
    };

    DataWorker.prototype.alterColumnName = function (oldName, newName) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "alterColumnName",
                oldName : oldName,
                newName : newName
            });
        });

        return self;
    };

    DataWorker.prototype.alterColumnSortType = function (column, sortType) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd      : "alterColumnSortType",
                column   : column,
                sortType : sortType
            });
        });

        return self;
    };

    DataWorker.prototype.alterColumnAggregateType = function (column, aggType) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "alterColumnAggType",
                column  : column,
                aggType : aggType
            });
        });

        return self;
    };

    DataWorker.prototype.alterColumnTitle = function (column, title) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd    : "alterColumnTitle",
                column : column,
                title  : title
            });
        });

        return self;
    };

    DataWorker.prototype.group = function () {
        var self = this,
            groupBy = _getArray(arguments);

        self._queueNext(function () {
            self._postMessage({
                cmd        : "group",
                keyColumns : groupBy
            });
        });

        return self;
    };

    DataWorker.prototype.partition = function () {
        var self = this,
            partitionBy = _getArray(arguments);

        self._queueNext(function () {
            self._postMessage({
                cmd        : "partition",
                keyColumns : partitionBy
            });
        });

        return self;
    };

    DataWorker.prototype.getPartitionKeys = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "getPartitionKeys" });
        })._queueNext(function () {
            callback.call(self, self._keys);
            self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getPartitioned = function () {
        var self = this,
            callback = arguments[0],
            keys = _getArray(arguments, 1);

        self._queueNext(function () {
            self._postMessage({
                cmd : "getPartitioned",
                key : keys.join("|")
            });
        })._queueNext(function () {
            callback.call(self, self._rows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.sortPartition = function () {
        var self = this, partitionKey = arguments[0], sortColumns = _getArray(arguments, 1);

        self._queueNext(function () {
            self._postMessage({
                cmd    : "sortPartition",
                key    : partitionKey,
                sortOn : sortColumns
            });
        });

        return self;
    };

    DataWorker.prototype.render = function (renderFunction) {
        var self = this;

        if (typeof(renderFunction) === "function") {
            self._queueNext(function () {
                self._renderFunction = renderFunction;
                return self._finishAction();
            });
        } else {
            self._queueNext(function () {
                self._renderFunction();
                return self._finishAction();
            });
        }

        return self;
    };

    DataWorker.prototype.onError = function (cb, actImmediately) {
        var self = this;

        if (actImmediately) {
            self._onError = cb;
        } else {
            self._queueNext(function () {
                self._onError = cb;

                return self._finishAction();
            });
        }

        return self;
    };

    DataWorker.prototype.onReceiveColumns = function (callback, actImmediately) {
        var self = this;

        var wrappedCallback = function () {
            self._onReceiveColumnsTracker = false;
            callback.apply(this, arguments);
        };

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._onReceiveColumns = wrappedCallback;

                if (self._onReceiveColumnsTracker) {
                    callback.call(self, self._columns, self._expectedNumRows);
                }
            } else {
                self._queueNext(function () {
                    self._onReceiveColumns = wrappedCallback;

                    if (self._onReceiveColumnsTracker) {
                        callback.call(self, self._columns, self._expectedNumRows);
                    }

                    return self._finishAction();
                });
            }
        }

        return self;
    };

    DataWorker.prototype.onReceiveRows = function (callback, actImmediately) {
        var self = this;

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._onReceiveRows = callback;
            } else {
                self._queueNext(function () {
                    self._onReceiveRows = callback;
                    return self._finishAction();
                });
            }
        }

        return self;
    };

    DataWorker.prototype.onTrigger = function (callback, actImmediately) {
        var self = this;

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._onTrigger = callback;
            } else {
                self._queueNext(function () {
                    self._onTrigger = callback;
                    return self._finishAction();
                });
            }
        }

        return self;
    };

    DataWorker.prototype.onAllRowsReceived = function (callback, actImmediately) {
        var self = this;

        var wrappedCallback = function () {
            self._onAllRowsReceivedTracker = false;
            callback.apply(this, arguments);
        };

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._onAllRowsReceived = wrappedCallback;

                if (self._onAllRowsReceivedTracker) {
                    callback.call(self);
                }
            } else {
                self._queueNext(function () {
                    self._onAllRowsReceived = wrappedCallback;

                    if (self._onAllRowsReceivedTracker) {
                        callback.call(self);
                    }

                    return self._finishAction();
                });
            }
        }

        return self;
    };

    DataWorker.prototype.getNumberOfRecords = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "getNumberOfRecords" });
        })._queueNext(function () {
            callback.call(self, self._numRows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.clone = function (callback) {
        var self = this;

        self.getAllColumnsAndAllRecords(function (columns, records) {
            var columnsRow = Object.keys(columns).map(function (columnName) {
                return columns[columnName];
            }).sort(function (a, b) {
                return a.index - b.index;
            });

            var dataset = [ columnsRow ].concat(records);

            callback.call(self, new DataWorker(dataset));
        }, true);

        return self;
    };

    DataWorker.prototype.getExpectedNumberOfRecords = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "getExpectedNumRows" });
        })._queueNext(function () {
            callback.call(self, self._expectedNumRows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.requestDataset = function (request, forAppend) {
        var self = this,
            cmd  = "requestDataset";

        if (forAppend) cmd += "ForAppend";
        else self.cancelOngoingRequests();

        self._queueNext(function () {
            self._onAllRowsReceivedTracker = false;
            self._onReceiveColumnsTracker  = false;

            self._postMessage({ cmd: cmd, request: request });
        });

        return self;
    };

    DataWorker.prototype.requestDatasetForAppend = function (request) {
        return this.requestDataset(request, true);
    };

    DataWorker.prototype._hideShowColumns = function (cmd, columnNames) {
        var self = this, msg = { cmd: cmd };

        if (typeof(columnNames[0]) === "string") {
            msg.columnNames = columnNames;
        } else if (columnNames[0] instanceof RegExp) {
            msg.columnNameRegex = columnNames[0];
        } else if ("property" in columnNames[0] && "value" in columnNames[0]) {
            msg.property = columnNames[0];
        }

        self._queueNext(function () {
            self._postMessage(msg);
        });

        return self;
    };

    DataWorker.prototype.hideColumns = function () {
        var self = this, columnNames = _getArray(arguments);

        self._hideShowColumns("hideColumns", columnNames);

        return self;
    };

    DataWorker.prototype.showColumns = function () {
        var self = this, columnNames = _getArray(arguments);

        self._hideShowColumns("showColumns", columnNames);

        return self;
    };

    DataWorker.prototype.hideAllColumns = function () {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "hideAllColumns" });
        });

        return self;
    };

    DataWorker.prototype.showAllColumns = function () {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "showAllColumns" });
        });

        return self;
    };

    DataWorker.prototype.getAllColumns = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "getAllColumns" });
        })._queueNext(function () {
            callback.call(self, self._columns);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.addChildRows = function (data, joinColumn) {
        var self = this;

        self._queueNext(function () {
            var callback = function (rows) {
                self._postMessage({
                    cmd    : "addChildRows",
                    joinOn : joinColumn,
                    rows   : rows
                });
            };

            if (data instanceof DataWorker) {
                data.getRows(function (rows) {
                    callback.call(self, rows);
                });
            } else {
                callback.call(self, data);
            }
        });

        return self;
    };

    DataWorker.prototype.postMessage = function (message) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({
                cmd     : "postMessage",
                message : message
            });
        });

        return self;
    };

    DataWorker.prototype.getSummaryRows = function (callback) {
        var self = this,
            columnNames = _getArray(arguments, 1);

        self._queueNext(function () {
            self._postMessage({
                cmd         : "getSummaryRows",
                columnNames : columnNames
            });
        })._queueNext(function () {
            callback.call(self, self._summaryRows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.setSummaryRows = function () {
        var self = this,
            rows = arguments.length === 1 ?
                arguments[0] : Array.prototype.slice.call(arguments);

        self._queueNext(function () {
            self._postMessage({
                cmd         : "setSummaryRows",
                summaryRows : rows
            });
        });
    };

    DataWorker.prototype.clearDataset = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "clearDataset" });
        });

        return self;
    };

    DataWorker.prototype.cancelOngoingRequests = function () {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "cancelOngoingRequests" });
        });

        return self;
    };

    DataWorker.prototype.search = function (callback, filters, options) {
        var self = this;

        if (typeof options === "undefined") {
            options = {};
        }

        if (typeof filters === "string" || filters instanceof RegExp) {
            filters = [ filters, options.searchOn || options.columns ]
                .filter(function (term) { return !!term; });
        }


        self._queueNext(function () {
            self._postMessage({
                cmd           : "search",
                filters       : filters,
                columns       : options.columns,
                allRows       : options.allRows,
                getDistinct   : options.getDistinct,
                sortOn        : options.sortOn,
                searchOn      : options.searchOn,
                returnColumns : options.returnColumns,
                fromRow       : options.fromRow || 0,
                limit         : options.limit
            });
        })._queueNext(function () {
            callback.call(self, self._rows);
            self._finishAction();
        });

        return self;
    };

    function _getArray(args, offset) {
        var idx = offset || 0;

        return args[idx] instanceof Array ? args[idx] : Array.prototype.slice.call(args, idx);
    }
})();
