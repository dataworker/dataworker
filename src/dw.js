(function() {
    "use strict";

    var srcPath = (function () {
        var scripts = document.getElementsByTagName("script"),
            srcFile = scripts[scripts.length - 1].src;

        return srcFile.replace(
            /(https?:\/\/)?.*?(\/(.*\/)?).*/,
            function () { return arguments[2]; }
        );
    })();

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
        self._initializeWebWorker(dataset);

        return self;
    };

    DataWorker.workerPool = new WebWorkerPool(srcPath + "dw-helper.js");
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
                dataset["onReceiveColumns"].apply(this, arguments);
            };
        } else {
            self._onReceiveColumns = function () {};
        }

        self._onAllRowsReceivedTracker = false;
        if ("onAllRowsReceived" in dataset) {
            self._onAllRowsReceived = function () {
                self._onAllRowsReceivedTracker = false;
                dataset["onAllRowsReceived"].apply(this, arguments);
            };
        } else {
            self._onAllRowsReceived = function () {};
        }

        self._onReceiveRows = "onReceiveRows" in dataset
                              ? dataset["onReceiveRows"]
                              : function () {};

        self._onTrigger = "onTrigger" in dataset
                         ? dataset["onTrigger"]
                         : function () {};

        self._onError = "onError" in dataset ? dataset["onError"] : function () {};

        return self;
    };

    DataWorker.prototype._initializeWebWorker = function (dataset) {
        var self = this, columns, rows, datasource, request;

        if (dataset instanceof Array) {
            columns = dataset.slice(0, 1)[0];
            rows    = dataset.slice(1);
        } else {
            datasource = dataset.datasource;

            request = (typeof dataset.request === "string")
                ? dataset.request
                : JSON.stringify(dataset.request);
        }

        self._queueNext(function () {
            self._isSingleThreaded = (
                typeof(Worker) === "undefined"
                || dataset.forceSingleThread
            );

            self._worker = self._isSingleThreaded
                ? ( new DataWorkerHelper() )
                : self.workerPool.getWorker();

            self._worker.onmessage = function (e) {
                if (!e.data) return;

                if ("rowsReceived" in e.data) {
                    self._onReceiveRows(e.data.rowsReceived);
                    return;
                }
                if ("triggerMsg" in e.data) {
                    self._onTrigger(e.data.triggerMsg);
                    return;
                }
                if ("allRowsReceived" in e.data) {
                    self._onAllRowsReceivedTracker = true;
                    self._onAllRowsReceived();
                    return;
                }

                if ("currentPage" in e.data) {
                    self._currentPage = e.data.currentPage;
                }

                if ("numberOfPages" in e.data) {
                    self._numberOfPages = e.data.numberOfPages;
                }

                if ("error"        in e.data) self._onError(e.data.error);

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
                    self._onReceiveColumns(self._columns, self._expectedNumRows);
                    return;
                }

                self._finishAction(true);
            };

            self._postMessage({
                cmd                    : "initialize",

                columns                : columns,
                rows                   : rows,

                datasource             : datasource,
                authenticate           : dataset.authenticate,
                request                : request,
                onClose                : dataset.onClose,
                shouldAttemptReconnect : dataset.shouldAttemptReconnect
            });
        });

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
            callback(self._columns);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getDataset = function () {
        var self = this,
            callback = arguments[0],
            columnNames = _getArray(arguments, 1);

        self._queueNext(function () {
            self._postMessage({
                cmd         : "getDataset",
                columnNames : columnNames
            });
        })._queueNext(function () {
            callback(self._rows);
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
            callback(self._distinctRows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.applyFilter = function () {
        var self = this,
            filters = _getArray(arguments);

        self._queueNext(function () {
            self._postMessage({
                cmd     : "applyFilter",
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

    DataWorker.prototype.filter = function () {
        var self = this,
            regex = arguments[0],
            relevantColumns = _getArray(arguments, 1);

        self._queueNext(function () {
            self._postMessage({
                cmd             : "filter",
                regex           : regex,
                relevantColumns : relevantColumns
            });
        });

        return self;
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
            callback(self._rows, self._currentPage);
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
            callback(self._rows, self._currentPage);
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
            callback(self._numberOfPages);
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
            callback(self._rows, self._currentPage);
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
            callback(self._rows);
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
            callback(self._rows);
            self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getColumnsAndRecords = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "refresh" });
        })._queueNext(function () {
            callback(self._columns, self._rows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.getAllColumnsAndAllRecords = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "refreshAll" });
        })._queueNext(function () {
            callback(self._columns, self._rows);
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
                });
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
                typeof(joinType) !== "undefined"
                && !(joinType === "left" || joinType === "right" || joinType === "inner")
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
            callback(self._hash);
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
            callback(self._keys);
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
            callback(self._rows);
            return self._finishAction();
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
                    callback(self._columns, self._expectedNumRows);
                }
            } else {
                self._queueNext(function () {
                    self._onReceiveColumns = wrappedCallback;

                    if (self._onReceiveColumnsTracker) {
                        callback(self._columns, self._expectedNumRows);
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
                    callback();
                }
            } else {
                self._queueNext(function () {
                    self._onAllRowsReceived = wrappedCallback;

                    if (self._onAllRowsReceivedTracker) {
                        callback();
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
            callback(self._numRows);
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
                return a["index"] - b["index"];
            });

            var dataset = [ columnsRow ].concat(records);

            callback(new DataWorker(dataset));
        });

        return self;
    };

    DataWorker.prototype.getExpectedNumberOfRecords = function (callback) {
        var self = this;

        self._queueNext(function () {
            self._postMessage({ cmd : "getExpectedNumRows" });
        })._queueNext(function () {
            callback(self._expectedNumRows);
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

    DataWorker.prototype.hideColumns = function () {
        var self = this, msg = { cmd : "hideColumns" },
            columnNames = _getArray(arguments);

        if (columnNames[0] instanceof RegExp) {
            msg["columnNameRegex"] = columnNames[0];
        } else {
            msg["columnNames"] = columnNames;
        }

        self._queueNext(function () {
            self._postMessage(msg);
        });

        return self;
    };

    DataWorker.prototype.showColumns = function () {
        var self = this, msg = { cmd : "showColumns" },
            columnNames = _getArray(arguments);

        if (columnNames[0] instanceof RegExp) {
            msg["columnNameRegex"] = columnNames[0];
        } else {
            msg["columnNames"] = columnNames;
        }

        self._queueNext(function () {
            self._postMessage(msg);
        });

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
            callback(self._columns);
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
                data.getDataset(function (rows) {
                    callback(rows);
                });
            } else {
                callback(data);
            }
        });

        return self;
    };

    DataWorker.prototype.then = function (callback) {
        var self = this;

        self._queueNext(function () {
            callback();
            return self._finishAction();
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
            callback(self._summaryRows);
            return self._finishAction();
        });

        return self;
    };

    DataWorker.prototype.setSummaryRows = function () {
        var self = this,
            rows = arguments.length === 1
                 ? arguments[0]
                 : Array.prototype.slice.call(arguments);

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
                .filter(function (term) { return !!term })
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
            callback(self._rows);
            self._finishAction();
        });

        return self;
    };

    function _getArray(args, offset) {
        var idx  = offset || 0,
            arr  = args[idx] instanceof Array
                 ? args[idx]
                 : Array.prototype.slice.call(args, idx);

        return arr;
    }
})();
