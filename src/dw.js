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
                protocol   = (/^(wss?|https?|file):/.exec(datasource) || [])[1];

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
