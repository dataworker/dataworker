(function (globalWorker) {
"use strict";

    var helper;

    var DWH = function DataWorkerHelper() {
        var self = this instanceof DWH ? this : Object.create(DWH.prototype);

        self._columns     = {};
        self._rows        = [];
        self._summaryRows = [];

        self._partitionedBy   = [];
        self._partitionedRows = {};

        self._expectedNumRows;

        self._datasources;

        self._wsDatasource;
        self._wsAuthenticate;
        self._socket;
        self._isWsReady;

        self._cancelRequestsCmd;
        self._cancelRequestsAck;
        self._waitForCancelRequestsAck = false;
        self._onSocketClose;
        self._shouldAttemptReconnect = false;

        self._ajaxDatasource;
        self._ajaxAuthenticate;
        self._ajaxRequests = [];
        self._ajaxRequestCounter = 0;

        self._shouldClearDataset = false;

        self._rowsPerPage = 10;
        self._currentPage = undefined;

        self._onlyValidForNumbersRegex = /[^0-9.e\-]/g;

        self._hasChildElements = false;

        self._isFinished = false;

        return self;
    };

    DWH.prototype.handleMessage = function (data) {
        var self = this, reply = {};

        if (typeof(data) === "undefined") return;

        if (data.cmd === "initialize") {
            var waitToConnect = self.initialize(data);

            if (waitToConnect) {
                var wait = function () {
                    if (self._isWsReady) {
                        self._postMessage(reply);
                    } else {
                        setTimeout(wait, 500);
                    }
                };
                setTimeout(wait, 250);

                return;
            }
        } else if (data.cmd === "postMessage") {
            self._sendThroughWebSocket(data);
        } else {
            if (data.cmd in self) {
                reply = self[data.cmd](data);
            } else {
                reply["error"] = "Unrecognized DataWorker command: " + data.cmd;
            }
        }

        if (reply) self._postMessage(reply);
    };

    if (typeof(window) === "undefined") {
        // Running in WebWorker.

        DWH.prototype._postMessage = function _postMessage(reply) {
            if (!this._isFinished) globalWorker.postMessage(reply);
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
        DWH.prototype._postMessage = function (reply) {
            var self = this;

            setTimeout(function () {
                if (!self._isFinished) self.onmessage({ data: reply });
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
            if (typeof(self._onSocketClose) !== "undefined") {
                self._socket.send(self._onSocketClose);
            }

            self._socket.onclose = function () {};
            self._socket.close();
        }

        self._postMessage({});

        self._isFinished = true;
    };

    /* Initializations */

    function stringify(o) { return typeof(o) === "string" ? o : JSON.stringify(o); }

    DWH.prototype.initialize = function (data, useBackup) {
        var self = this,
            waitToConnect = false,
            datasource;

        if (typeof(self._datasources) === "undefined") {
            self._datasources = (data.datasource instanceof Array)
                        ?   data.datasource.slice(0)
                        : [ data.datasource ];

            self._datasources = self._datasources.map(function (datasource) {
                return typeof(datasource) === "string"
                    ? { source: datasource }
                    : datasource;
            });
        }

        self._wsDatasource = self._socket = self._ajaxDatasource = undefined;

        try {
            if (useBackup) {
                if (self._datasources.length > 0) {
                    self._postMessage({
                        warning: "Datasource failed. Falling back to next datasource."
                    });
                } else {
                    self._postMessage({ error: "Error: No available datasources." });
                    return false;
                }
            }

            datasource = self._datasources.shift();

            if (typeof(datasource) === "undefined") {
                self._columns = self._prepareColumns(data.columns);
                self._rows    = self._prepareRows(data.rows);
            } else if (/^https?:\/\//.test(datasource.source)) {
                self._ajaxDatasource   = datasource.source;
                self._ajaxAuthenticate = datasource.authenticate || data.authenticate;

                if (typeof(data.request) !== "undefined") {
                    self.requestDataset(data);
                }
            } else if (/^wss?:\/\//.test(datasource.source)) {
                self._wsDatasource      = datasource.source;
                self._wsAuthenticate    = stringify(
                    datasource.authenticate || data.authenticate
                );
                self._cancelRequestsCmd = stringify(datasource.cancelRequestsCmd);
                self._cancelRequestsAck = stringify(datasource.cancelRequestsAck);

                self._shouldAttemptReconnect = data.shouldAttemptReconnect;

                waitToConnect = self._initializeWebsocketConnection(data);
            } else {
                self._postMessage({
                    error: "Error: Could not initialize DataWorker; unrecognized datasource."
                });
            }
        } catch (error) {
            if (self._datasources.length) {
                waitToConnect = self.initialize(data, true);
            } else {
                self._postMessage({ error : "Error: " + error });
            }
        }

        return waitToConnect;
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

    DWH.prototype._ajax = function (request) {
        var self = this,
            xmlHttp = new XMLHttpRequest(),
            url = self._ajaxDatasource + (/^\?/.test(request) ? "" : "?"),
            requestCount = self._ajaxRequestCounter;

        self._ajaxRequests.push(xmlHttp);

        url += self._getRequestParams(request);
        url += self._getRequestParams(self._ajaxAuthenticate);

        xmlHttp.onreadystatechange = function () {
            if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
                if (requestCount !== self._ajaxRequestCounter) return;

                var msg = JSON.parse(xmlHttp.responseText);

                if (msg.error) {
                    self._postMessage({ error : msg.error });
                }

                if (msg.columns) {
                    if (self._shouldClearDataset) self.clearDataset();

                    var error = self._checkColumnsForAppend(msg.columns);
                    if (error) return self._postMessage({ error: error });
                }
                if (msg.rows) {
                    if (typeof(self._rows) === "undefined") self._rows = [];
                    self._rows = self._rows.concat(self._prepareRows(msg.rows));
                }
                if (msg.summaryRows) {
                    if (typeof(self._summaryRows) === "undefined") self._summaryRows = [];
                    self._summaryRows = self._summaryRows.concat(self._prepareRows(msg.summaryRows));
                }

                self._postMessage({
                    columnsReceived : true,
                    columns         : self._getVisibleColumns(),
                    exNumRows       : self._getVisibleRows().length
                });

                self._postMessage({ allRowsReceived : true });

                if (msg.trigger) self._postMessage({ triggerMsg: msg.msg });
            }
        };

        xmlHttp.open("GET", url, true);
        xmlHttp.send();
    };

    DWH.prototype._initializeWebsocketConnection = function (data) {
        var self = this;

        self._socket = new WebSocket(self._wsDatasource);
        self._isWsReady = false;

        self._socket.onopen  = function () {
            if (self._wsAuthenticate) {
                self._socket.send(self._wsAuthenticate);
            }

            if (typeof(data.request) !== "undefined") {
                self._socket.send(data.request);
            }

            self._isWsReady = true;
        };
        self._socket.onclose = function (e) {
            if (
                self._shouldAttemptReconnect
                && e.code !== 1000 && e.code !== 1001
            ) {
                self._initializeWebsocketConnection(data);
            }
        };
        self._socket.onerror = function (error) {
            if (error.target.readyState === 0 || error.target.readyState === 3) {
                self._isWsReady = true;
                self._socket = null;

                self.initialize(data, true);
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

            try {
                msg = JSON.parse(msg.data);
            } catch (error) {
                self._postMessage({ "Error ": error.message + ": " + msg.data });
            }

            if (msg.error) {
                self._postMessage({ error : msg.error });
                return;
            }

            if (msg.columns) {
                if (self._shouldClearDataset) self.clearDataset();

                var error = self._checkColumnsForAppend(msg.columns);
                if (error) return _postMessage({ error: error });
            }

            if (msg.expectedNumRows === "INFINITE") {
                self._expectedNumRows = msg.expectedNumRows;
            } else if (typeof(msg.expectedNumRows) !== "undefined") {
                if (typeof(expectedNumRows) === "undefined") {
                    self._expectedNumRows = parseInt(msg.expectedNumRows);
                } else {
                    self._expectedNumRows += parseInt(msg.expectedNumRows);
                }
            }

            if (msg.rows) {
                var preparedRows = self._prepareRows(msg.rows);

                if (typeof(self._rows) === "undefined") self._rows = [];
                self._rows.push.apply(self._rows, preparedRows);

                if (self._partitionedBy.length > 0) {
                    self._insertIntoPartitionedRows(preparedRows);
                }

                if (self._expectedNumRows !== "INFINITE" && self._rows.length == self._expectedNumRows) {
                    self._postMessage({ allRowsReceived : true });
                } else {
                    self._postMessage({ rowsReceived : msg.rows.length });
                }
            } else if (
                typeof(self._columns) !== "undefined"
                && typeof(msg.expectedNumRows) !== "undefined"
                && self._expectedNumRows != self._rows.length
            ) {
                self._postMessage({
                    columnsReceived : true,
                    columns         : self._getVisibleColumns(),
                    exNumRows       : self._expectedNumRows
                });
            }

            if (msg.summaryRows) {
                if (typeof(self._summaryRows) === "undefined") self._summaryRows = [];
                self._summaryRows = self._summaryRows.concat(self._prepareRows(msg.summaryRows));
            }

            if (msg.expectedNumRows == 0) self._postMessage({ allRowsReceived : true });

            if (msg.trigger) self._postMessage({ triggerMsg : msg.msg });

            self._onSocketClose = data.onClose;
        };

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
                    rowsWithChildren.push.apply(rowsWithChildren, row.children);
                }
            });

            requestedRows = rowsWithChildren;
        }

        (requestedColumns || Object.keys(self._columns)).forEach(function (columnName) {
            var column = self._columns[columnName];

            if (column && (requestedColumns || column["isVisible"])) {
                visibleColumnIdxs.push(column.index);
            }
        });

        requestedRows.forEach(function (row) {
            if (row["isVisible"] || allRows) {
                var newRow = visibleColumnIdxs.map(function (idx) {
                    return row["row"][idx];
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
            numberCols[self._columns[name].index] = self._columns[name]["sortType"] === "num";
        });

        if (typeof filters[0] === "string" || filters[0] instanceof RegExp) {
            filters = [ {
                columns: filters[1] instanceof Array ? filters[1] : filters.slice(1),
                regex: RegExp(filters[0])
            } ];
        }

        filters = filters.map(function (filter) {
            if (filter.column) filter.columns = [ filter.column ];
            if (typeof filter.columns === "string") filter.columns = [ filter.columns ];
            if (filter.columns && filter.columns.length) {
                filter.indices = filter.columns.reduce(function (indices, columnName) {
                    var column = self._columns[columnName];
                    if (column) indices.push(column["index"]);

                    return indices;
                }, []);
            } else {
                filter.indices = allIndices;
            }

            filter.tests = [ "eq", "ne", "gte", "gt", "lte", "lt", "regex", "!regex" ]
                .filter(function (name) { return name in filter });

            [ "regex", "!regex" ].forEach(function (type) {
                if (filter[type]) filter[type] = RegExp(filter[type]);
            });

            return filter;
        }).filter(function (filter) { return filter.indices.length; });

        return self._rows.reduce(function (results, row) {
            if (!row["isVisible"] && !data.allRows) return results;

            var validRow = filters.every(function (filter) {
                return filter.indices[filter.matchAll ? "every" : "some"](function (index) {
                    return filter.tests.every(function (test) {
                        return self._testCell(
                            row["row"][index],
                            filter,
                            numberCols[index],
                            test
                        );
                    });
                });
            });

            if (validRow) {
                if (data.setVisibility) {
                    row["isVisible"] = true;
                } else {
                    results.push(row);
                }
            } else if (data.setVisibility) {
                row["isVisible"] = false;
            }

            return results;
        }, []);
    };

    DWH.prototype._testCell = function (cell, filter, isNum, testName) {
        if (isNum) cell = parseFloat(cell);

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

    DWH.prototype._extractColumnNamesInOrder = function (columns) {
        var self = this;

        return Object.keys(columns).sort(function (a, b) {
            return self._numSort(
                columns[a]["index"],
                columns[b]["index"]
            );
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

            return "Cannot append dataset (columns do not match):\n\t"
                   + originalColumns + "\n\t\tVS\n\t" + appendColumns;
        };

        if (Object.keys(newColumns).length === Object.keys(self._columns).length) {
            var columnNames = Object.keys(newColumns);

            for (i = 0; i < columnNames.length; i++) {
                var name = columnNames[i];

                if (typeof(self._columns[name]) === "undefined") {
                    return errorMsg();
                } else if (newColumns[name]["index"] !== self._columns[name]["index"]) {
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
                return self._columns[columnName]["index"];
            });

        myRows.forEach(function (row) {
            var rowValues = row["row"],
                key = keyIndexes.map(function (i) { return rowValues[i]; }).join("|");

            if (key in hash) {
                hash[key].push(preparedRows ? row : rowValues);
            } else {
                hash[key] = [ preparedRows ? row : rowValues ];
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
            if (!column in self._columns) {
                errors.push("Column \"" + column + "\" not in dataset.");
            }
        });

        self._hashRowsByKeyColumns(keyColumns, self._rows, hash, preparedRows);

        reply["hash"] = hash;
        if (errors.length) reply["error"] = errors.join("\n");

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
        var self = this;

        if ("columnNames" in data) {
            data.columnNames.forEach(function (column) {
                if (column in self._columns) {
                    self._columns[column]["isVisible"] = isVisible;
                }
            });
        } else if ("columnNameRegex" in data) {
            Object.keys(self._columns).forEach(function (column) {
                if (RegExp(data.columnNameRegex).test(column)) {
                    self._columns[column]["isVisible"] = isVisible;
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
                sortType   = self._columns[columnName]["sortType"];

                if (reverse) {
                    valB = a["row"][self._columns[columnName]["index"]];
                    valA = b["row"][self._columns[columnName]["index"]];
                } else {
                    valA = a["row"][self._columns[columnName]["index"]];
                    valB = b["row"][self._columns[columnName]["index"]];
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
            row["isVisible"] = true;
        });

        return {};
    };

    DWH.prototype.filter = function (data) {
        var self = this,
            regex = RegExp(data.regex), relevantColumns = data.relevantColumns,
            i, column, filteredDataset = [],
            relevantIndexes = relevantColumns.map(function (column) {
                return self._columns[column]["index"];
            });

        self._rows.forEach(function (row) {
            for (i = 0; i < row["row"].length; i++) {
                column = row["row"][i];

                if (
                    (relevantIndexes.length === 0 || relevantIndexes.indexOf(i) !== -1)
                    && column !== null && regex.test(column)
                ) {
                    filteredDataset.push(row);
                    break;
                }
            }
        });

        self._rows = filteredDataset;

        return {};
    };

    DWH.prototype.applyLimit = function (data) {
        var self = this,
            numRows = data.numRows, i = 0;

        self._rows.forEach(function (row) {
            if (i++ < numRows) {
                row["isVisible"] = true;
            } else {
                row["isVisible"] = false;
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
                column["index"] = i;

                columnsToKeep[columnName] = column;
            }
        });

        self._rows.forEach(function (row) {
            var filteredRow = [];

            Object.keys(columnsToKeep).forEach(function (columnName) {
                filteredRow[columnsToKeep[columnName]["index"]]
                    = row["row"][self._columns[columnName]["index"]];
            });

            row["row"] = filteredRow;;
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

        self._columns[column]["sortType"] = sortType;

        return {};
    };

    DWH.prototype.alterColumnAggType = function (data) {
        var self = this,
            column = data.column, aggType = data.aggType;

        self._columns[column]["aggType"] = aggType;

        return {};
    };

    DWH.prototype.alterColumnTitle = function (data) {
        var self = this,
            column = data.column, title = data.title;

        self._columns[column]["title"] = title;

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
            column["index"] += originalNumColumns;

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
                    var aggType = self._columns[columnName]["aggType"],
                        index = self._columns[columnName]["index"],
                        isKeyColumn = groupBy.indexOf(columnName) !== -1;

                    if (index in groupedRow && !isKeyColumn) {
                        if (aggType === "sum") {
                            groupedRow[index] += row[index];
                        } else if (aggType === "max") {
                            groupedRow[index] = ( groupedRow[index] < row[index] )
                                               ? row[index]
                                               : groupedRow[index];
                        } else if (aggType === "min") {
                            groupedRow[index] = ( groupedRow[index] > row[index] )
                                               ? row[index]
                                               : groupedRow[index];
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
            return self._numSort(a["index"], b["index"]);
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
        var self = this;
        return { rows : self._getVisibleRows(undefined, self._partitionedRows[data.key]) };
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
            self._columns[column]["isVisible"] = false;
        });

        return {};
    };

    DWH.prototype.showAllColumns = function (data) {
        var self = this;

        Object.keys(self._columns).forEach(function (column) {
            self._columns[column]["isVisible"] = true;
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

        return requestMade
            ? {}
            : { error: "Could not request dataset; no datasource defined." };
    };

    DWH.prototype.addChildRows = function (data) {
        var self = this,
            newRows = self._prepareRows(data.rows),
            rowHash = self._hashRowsByKeyColumns([ data.joinOn ], newRows, {}, true),
            joinIdx = self._columns[data.joinOn].index;

        self._rows.forEach(function (row, i) {
            var rowValues = row.row,
                children   = rowHash[rowValues[joinIdx]];

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
        var self = this;

        return {
            columns : self._columns,
            rows    : self._rows.map(function (row) { return row.row; })
        };
    };

    DWH.prototype.cancelOngoingRequests = function (data) {
        var self = this;

        if (self._ajaxDatasource) {
            self._ajaxRequests.forEach(function (xhr) { xhr.abort() });
            self._ajaxRequests = [];
            self._ajaxRequestCounter++;
        } else if (self._wsDatasource) {
            if (
                self._socket !== undefined
                && self._cancelRequestsCmd !== undefined
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
            typeof(self._socket) !== "undefined"
            && self._socket.readyState !== 0
            && self._socket.readyState !== 3
        ) {
            self._socket.send(data.message);
        }
    };
})(this);
