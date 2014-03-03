(function () {
    "use strict";

    var handleMessage;

    if (typeof window === "undefined") {
        runWorker(self);
        self.addEventListener("message", handleMessage, false);
    } else {
        var DataWorkerHelper = window.DataWorkerHelper = function DataWorkerHelper () {
            var self = this instanceof DataWorkerHelper
                     ? this
                     : Object.create(DataWorkerHelper.prototype);

            try {
                runWorker(self);
            } catch (error) {
                self.onerror(error);
            }

            return self;
        };

        DataWorkerHelper.prototype.postMessage = function (message, transferList) {
            var self = this;

            setTimeout(function () {
                try {
                    handleMessage({ data : message });
                } catch (error) {
                    self.onerror(error);
                }
            }, 0);

            return self;
        };

        DataWorkerHelper.prototype.onmessage = function (e) { return this; };

        DataWorkerHelper.prototype.onerror = function (e) {
            if (typeof console !== "undefined") console.error(e);
            return this;
        };

        DataWorkerHelper.prototype.terminate = function () { delete this; };
    }

    function runWorker(self) {

        var columns = {}, rows = [], summaryRows = [],
            partitionedBy = [], partitionedRows = {},
            socket, onSocketClose, isWsReady, expectedNumRows,
            ajaxDatasource, wsDatasource, datasources,
            rowsPerPage = 10, currentPage = undefined,
            onlyValidForNumbersRegex = /[^0-9.\-]/g,
            authentication,
            hasChildElements = false,
            postMessage;

        if (typeof window === "undefined") {
            postMessage = function () { return self.postMessage.apply(self, arguments); };
        } else {
            postMessage = function (message) {
                return self.onmessage({ data: message });
            };
        }

        var _prepareColumns = function (rawColumns) {
            var preparedColumns = {};

            rawColumns.forEach(function (column, i) {
                if (typeof(column) === "string") {
                    column = { name: column };
                }

                column.sortType  = column.sortType || "alpha";
                column.aggType   = column.aggType  || "max";
                column.title     = column.title    || column.name;
                column.index     = i;
                column.isVisible = ("isVisible" in column) ? column.isVisible : true;

                preparedColumns[column.name] = column;
            });

            return preparedColumns;
        };

        var _prepareRows = function (rawRows) {
            return rawRows.map(function (row) {
                return {
                    row       : row,
                    isVisible : true
                };
            });
        };

        var _stripRowMetadata = function (processedRows) {
            return processedRows.map(function (row) { return row["row"]; });
        };

        var _getVisibleColumns = function () {
            var visibleColumns = {}, index = 0;

            Object.keys(columns).sort(function (a, b) {
                return columns[a].index - columns[b].index;
            }).forEach(function (columnName) {
                if (columns[columnName].isVisible) {
                    visibleColumns[columnName] = JSON.parse(JSON.stringify(columns[columnName]));

                    visibleColumns[columnName].index = index++;
                }
            });

            return visibleColumns;
        };

        var _getVisibleRows = function (requestedColumns, requestedRows) {
            var visibleColumnIdxs = [], visibleRows = [];

            if (!(requestedColumns || []).length) {
                requestedColumns = undefined;
            }
            requestedRows = requestedRows || rows;

            if (hasChildElements) {
                var rowsWithChildren = [];
                requestedRows.forEach(function (row) {
                    rowsWithChildren.push(row);
                    if (row.hasChildren) {
                        rowsWithChildren.push.apply(rowsWithChildren, row.children);
                    }
                });

                requestedRows = rowsWithChildren;
            }

            (requestedColumns || Object.keys(columns)).forEach(function (columnName) {
                var column = columns[columnName];

                if (requestedColumns || column.isVisible) {
                    visibleColumnIdxs.push(column.index);
                }
            });

            requestedRows.forEach(function (row) {
                if (row["isVisible"]) {
                    var newRow = visibleColumnIdxs.map(function (idx) {
                        return row["row"][idx];
                    });

                    newRow.parentRow = row.parentRow;

                    visibleRows.push(newRow);
                }
            });

            return visibleRows;
        };

        var _initialize = function (data, useBackup) {
            var waitToConnect = false,
                datasource;

            authentication = data.authenticate;
            if (typeof(datasources) === "undefined") {
                datasources = (data.datasource instanceof Array)
                            ?   data.datasource
                            : [ data.datasource ];
            }

            ajaxDatasource = wsDatasource = socket = undefined;

            try {
                if (useBackup) throw new Error();

                datasource = datasources.shift();

                if (typeof(datasource) === "undefined") {
                    columns = _prepareColumns(data.columns);
                    rows    = _prepareRows(data.rows);
                } else if (/^https?:\/\//.test(datasource)) {
                    ajaxDatasource = datasource;

                    if (typeof(data.request) !== "undefined") {
                        _requestDataset(data);
                    }
                } else if (/^wss?:\/\//.test(datasource)) {
                    wsDatasource = datasource;

                    waitToConnect = _initializeWebsocketConnection(data);
                } else {
                    postMessage({
                        error: "Error: Could not initialize DataWorker; unrecognized datasource."
                    });
                }
            } catch (error) {
                if (datasources.length) {
                    waitToConnect = _initialize(data);
                } else {
                    postMessage({
                        error : "Error: " + error
                    });
                }
            }

            return waitToConnect;
        };

        var _getRequestParams = function (params) {
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
        }

        var _ajax = function (request) {
            var xmlHttp = new XMLHttpRequest(),
                url = ajaxDatasource + (/^\?/.test(request) ? "" : "?");

            url += _getRequestParams(request);
            url += _getRequestParams(authentication);

            xmlHttp.onreadystatechange = function () {
                if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
                    var msg = JSON.parse(xmlHttp.responseText);

                    if (msg.error) {
                        postMessage({ error : msg.error });
                    }

                    if (msg.columns) {
                        var error = _checkColumnsForAppend(msg.columns);
                        if (error) return postMessage({ error: error });
                    }
                    if (msg.rows) {
                        if (typeof(rows) === "undefined") rows = [];
                        rows = rows.concat(_prepareRows(msg.rows));
                    }
                    if (msg.summaryRows) {
                        if (typeof(summaryRows) === "undefined") summaryRows = [];
                        summaryRows = summaryRows.concat(_prepareRows(msg.summaryRows));
                    }

                    postMessage({
                        columnsReceived : true,
                        columns         : _getVisibleColumns(),
                        exNumRows       : _getVisibleRows().length
                    });

                    postMessage({ allRowsReceived : true });

                    if (msg.trigger) postMessage({ triggerMsg: msg.msg });
                }
            };

            xmlHttp.open("GET", url, true);
            xmlHttp.send();
        };

        var _initializeWebsocketConnection = function (data) {
            socket = new WebSocket(wsDatasource);
            isWsReady = false;

            socket.onopen  = function () {
                if (data.authenticate) {
                    socket.send(data.authenticate);
                }

                if (typeof(data.request) !== "undefined") {
                    socket.send(data.request);
                }

                isWsReady = true;
            };
            socket.onclose = function () {};
            socket.onerror = function (error) {
                if (error.target.readyState === 0 || error.target.readyState === 3) {
                    isWsReady = true;
                    socket = null;

                    _initialize(data, true);
                } else {
                    postMessage({
                        error : "Error: Problem with connection to datasource."
                    });
                }
            };

            socket.onmessage = function (msg) {
                try {
                    msg = JSON.parse(msg.data);
                } catch (error) {
                    postMessage({ "error": error.message + ": " + msg.data });
                }

                if (msg.error) {
                    postMessage({ error : msg.error });
                    return;
                }

                if (msg.columns) {
                    var error = _checkColumnsForAppend(msg.columns);
                    if (error) return postMessage({ error: error });
                }

                if (msg.expectedNumRows === "INFINITE") {
                    expectedNumRows = msg.expectedNumRows;
                } else if (typeof(msg.expectedNumRows) !== "undefined") {
                    if (typeof(expectedNumRows) === "undefined") {
                        expectedNumRows = parseInt(msg.expectedNumRows);
                    } else {
                        expectedNumRows += parseInt(msg.expectedNumRows);
                    }
                }

                if (msg.rows) {
                    var preparedRows = _prepareRows(msg.rows);

                    if (typeof(rows) === "undefined") rows = [];
                    rows.push.apply(rows, preparedRows);

                    if (partitionedBy.length > 0) {
                        _insertIntoPartitionedRows(preparedRows);
                    }

                    if (expectedNumRows !== "INFINITE" && rows.length == expectedNumRows) {
                        postMessage({ allRowsReceived : true });
                    } else {
                        postMessage({ rowsReceived : msg.rows.length });
                    }
                } else if (
                    typeof(columns) !== "undefined"
                    && typeof(msg.expectedNumRows) !== "undefined"
                    && expectedNumRows != rows.length
                ) {
                    postMessage({
                        columnsReceived : true,
                        columns         : _getVisibleColumns(),
                        exNumRows       : expectedNumRows
                    });
                }

                if (msg.summaryRows) {
                    if (typeof(summaryRows) === "undefined") summaryRows = [];
                    summaryRows = summaryRows.concat(_prepareRows(msg.summaryRows));
                }

                if (msg.expectedNumRows == 0) postMessage({ allRowsReceived : true });

                if (msg.trigger) postMessage({ triggerMsg : msg.msg });

                onSocketClose = data.onClose;
            };

            return true;
        };

        var _insertIntoPartitionedRows = function (preparedRows) {
            _hashRowsByKeyColumns(
                partitionedBy,
                preparedRows,
                partitionedRows,
                true
            );
        };

        var _alphaSort = function (a, b) {
            a = (a || "").toLowerCase();
            b = (b || "").toLowerCase();

            if (a < b) {
                return -1;
            } else if (a > b) {
                return 1;
            } else {
                return 0;
            }
        };

        var _localeAlphaSort = function (a, b) {
            a = (a || "").toLowerCase();
            b = (b || "").toLowerCase();

            return a.localeCompare(b);
        };

        var _numSort = function (a, b) {
            a += "";
            a = a.replace(onlyValidForNumbersRegex, "");
            a = parseFloat(a);

            b += "";
            b = b.replace(onlyValidForNumbersRegex, "");
            b = parseFloat(b);

            if (!isNaN(a) && !isNaN(b))
                return a - b;
            else if (isNaN(a) && isNaN(b))
                return 0;
            else
                return isNaN(a) ? -1 : 1;
        };

        var _sort = function (data) {
            var sortOn = data.sortOn,
                rowsToSort = data.rows || rows;

            function _compareRows(a, b) {
                var i, sortColumn, columnName, reverse, sortType, sortResult, valA, valB;

                for (i = 0; i < sortOn.length; i++) {
                    sortColumn = sortOn[i].match(/(-?)(\w+)/);
                    columnName = sortColumn[2];
                    reverse     = !!sortColumn[1];
                    sortType   = columns[columnName]["sortType"];

                    if (reverse) {
                        valB = a["row"][columns[columnName]["index"]];
                        valA = b["row"][columns[columnName]["index"]];
                    } else {
                        valA = a["row"][columns[columnName]["index"]];
                        valB = b["row"][columns[columnName]["index"]];
                    }

                    if (sortType === "alpha") {
                        sortResult = _alphaSort(valA, valB);
                    } else if (sortType === "localeAlpha") {
                        sortResult = _localeAlphaSort(valA, valB);
                    } else if (sortType === "num") {
                        sortResult = _numSort(valA, valB);
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

            if (hasChildElements) {
                rowsToSort.forEach(function (row) {
                    if (row.hasChildren) {
                        row.children.sort(_compareRows);
                    }
                });
            }

            return {};
        };

        var _setDecimalMarkCharacter = function (data) {
            var decimalMarkCharacter = data.decimalMarkCharacter;

            onlyValidForNumbersRegex = new RegExp("[0-9\-" + decimalMarkCharacter + "]", "g");

            return {};
        };

        var _search = function (data) {
            var results = _scanRows({
                setVisibility: false,
                filters: [ data.term, data.columns ].filter(function (term) { return !!term })
            });

            if (typeof data.sortOn  === "string") data.sortOn  = [ data.sortOn  ];
            if (typeof data.columns === "string") data.columns = [ data.columns ];

            if (data.sortOn) _sort({ rows: results, sortOn: data.sortOn });

            results = _getVisibleRows(data.columns, results);

            if (data.limit > 0) results = results.slice(0, data.limit);

            return { rows : results };
        };

        var _applyFilter = function (data) {
            data.setVisibility = true;
            _scanRows(data);

            return {};
        };

        var _scanRows = function (data) {
            var filters = data.filters, i = 0, column,
                isSimpleFilter = typeof filters[0] === "string" || filters[0] instanceof RegExp,
                results = [];

            if (isSimpleFilter) {
                var regex = RegExp(filters[0]),
                    relevantColumns = filters[1] instanceof Array ? filters[1] : filters.slice(1),
                    relevantIndexes = relevantColumns.map(function (column) {
                        return columns[column]["index"];
                    });

                rows.forEach(function (row) {
                    if (!row["isVisible"]) return;

                    if (data.setVisibility) {
                        row["isVisible"] = false;
                    }

                    for (i = 0; i < row["row"].length; i++) {
                        column = row["row"][i];

                        if (
                            (relevantIndexes.length === 0 || relevantIndexes.indexOf(i) !== -1)
                            && column && regex.test(column)
                        ) {
                            if (data.setVisibility) {
                                row["isVisible"] = true;
                            } else {
                                results.push(row);
                            }
                            break;
                        }
                    }
                });
            } else {
                rows.forEach(function (row) {
                    if (!row["isVisible"]) return;

                    if (data.setVisibility) {
                        row["isVisible"] = true;
                    }

                    for (i = 0; i < filters.length; i++) {
                        var filter = filters[i], columnIndex, column;

                        if (filter.column in columns) {
                            columnIndex = columns[filter.column]["index"],
                            column = row["row"][columnIndex];

                            if (column && filter.regex.test(column)) {
                                if (data.setVisibility) {
                                    row["isVisible"] = true;
                                } else {
                                    results.push(row);
                                }
                            } else {
                                if (data.setVisibility) {
                                    row["isVisible"] = false;
                                }
                                break;
                            }
                        } else {
                            break;
                        }
                    };
                });
            }

            return results;
        };

        var _clearFilters = function () {
            rows.forEach(function (row) {
                row["isVisible"] = true;
            });

            return {};
        };

        var _filter = function (data) {
            var regex = RegExp(data.regex), relevantColumns = data.relevantColumns;
            var i, column, filteredDataset = [],
                relevantIndexes = relevantColumns.map(function (column) {
                    return columns[column]["index"];
                });

            rows.forEach(function (row) {
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

            rows = filteredDataset;

            return {};
        };

        var _applyLimit = function (data) {
            var numRows = data.numRows;
            var i = 0;

            rows.forEach(function (row) {
                if (i++ < numRows) {
                    row["isVisible"] = true;
                } else {
                    row["isVisible"] = false;
                }
            });

            return {};
        };

        var _limit = function (data) {
            var numRows = data.numRows;

            rows = rows.slice(0, numRows);

            return {};
        };

        var _removeColumns = function (data) {
            var columnsToRemove = data.columnsToRemove;
            var i = 0, columnsToKeep = {}, filteredDataset = [];

            Object.keys(columns).forEach(function (columnName, i) {
                if (columnsToRemove.indexOf(columnName) === -1) {
                    var column = columns[columnName];
                    column["index"] = i;

                    columnsToKeep[columnName] = column;
                }
            });

            rows.forEach(function (row) {
                var filteredRow = [];

                Object.keys(columnsToKeep).forEach(function (columnName) {
                    filteredRow[columnsToKeep[columnName]["index"]]
                        = row["row"][columns[columnName]["index"]];
                });

                filteredDataset.push(filteredRow);
            });

            columns = columnsToKeep;
            rows    = _prepareRows(filteredDataset);

            return {};
        };

        var _prependColumnNames = function (data) {
            var prepend = data.prepend;
            var newColumns = {};

            Object.keys(columns).forEach(function (columnName) {
                newColumns[prepend + columnName] = columns[columnName];
            });

            columns = newColumns;

            return {};
        };

        var _alterColumnName = function (data) {
            var oldName = data.oldName, newName = data.newName;
            var error;

            Object.keys(columns).forEach(function (columnName) {
                if (columnName !== oldName && columnName === newName) {
                    error = "Column " + newName + " already exists in the dataset.";
                }
            });

            columns[newName] = columns[oldName];
            delete columns[oldName];

            return typeof(error) === "undefined" ? {} : { error : error };
        };

        var _alterColumnSortType = function (data) {
            var column = data.column, sortType = data.sortType;

            columns[column]["sortType"] = sortType;

            return {};
        };

        var _alterColumnAggType = function (data) {
            var column = data.column, aggType = data.aggType;

            columns[column]["aggType"] = aggType;

            return {};
        };

        var _alterColumnTitle = function (data) {
            var column = data.column, title = data.title;

            columns[column]["title"] = title;

            return {};
        };

        var _extractColumnNamesInOrder = function (columns) {
            return Object.keys(columns).sort(function (a, b) {
                return _numSort(
                    columns[a]["index"],
                    columns[b]["index"]
                );
            });
        };

        var _checkColumnsForAppend = function (newColumns) {
            var i, errorMsg;

            if (newColumns instanceof Array) {
                newColumns = _prepareColumns(newColumns);
            }

            if (Object.keys(columns).length === 0) {
                columns = newColumns;
                return;
            }

            errorMsg = function () {
                var originalColumns = _extractColumnNamesInOrder(columns) .join(", ");
                var appendColumns = _extractColumnNamesInOrder(newColumns) .join(", ");

                return "Cannot append dataset (columns do not match):\n\t"
                       + originalColumns + "\n\t\tVS\n\t" + appendColumns;
            };

            if (Object.keys(newColumns).length === Object.keys(columns).length) {
                var columnNames = Object.keys(newColumns);

                for (i = 0; i < columnNames.length; i++) {
                    var name = columnNames[i];

                    if (typeof(columns[name]) === "undefined") {
                        return errorMsg();
                    } else if (newColumns[name]["index"] !== columns[name]["index"]) {
                        return errorMsg();
                    }
                }
            } else {
                return errorMsg();
            }
        };

        var _append = function (data) {
            if (data.newColumns) {
                var error = _checkColumnsForAppend(data.newColumns);
                if (error) return { error: error };
            }

            rows.splice.apply(rows,
                [ "index" in data ? data.index : rows.length, 0 ].concat(
                    data.preparedRows || _prepareRows(data.newRows)
                )
            );

            return {};
        };

        var _hashRowsByKeyColumns = function (keyColumns, myRows, hash, preparedRows) {
            var keyIndexes = keyColumns.map(function (columnName) {
                return columns[columnName]["index"];
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

        var _hashDatasetByKeyColumns = function (data, preparedRows) {
            var keyColumns = data.keyColumns;
            var keyIndexes, hash = {}, errors = [], reply = {};

            keyColumns = keyColumns instanceof Array ? keyColumns : [ keyColumns ];
            keyColumns.forEach(function (column) {
                if (!column in columns) {
                    errors.push("Column \"" + column + "\" not in dataset.");
                }
            });

            _hashRowsByKeyColumns(keyColumns, rows, hash, preparedRows);

            reply["hash"] = hash;
            if (errors.length) reply["error"] = errors.join("\n");

            return reply;
        };

        var _joinHashes = function (data) {
            var lHash = _hashDatasetByKeyColumns(data), rHash = data.rHash,
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
                originalNumColumns = Object.keys(columns).length;

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

                columns[columnName] = column;
            });

            rows = _prepareRows(joinedDataset);

            return {};
        };

        var _group = function (data) {
            var hashedDataset = _hashDatasetByKeyColumns(data), groupBy = data.keyColumns;
            var groupedDataset = [], errors = [];

            if ("error" in hashedDataset) {
                return { "error" : hashedDataset.error };
            } else {
                hashedDataset = hashedDataset.hash;
            }

            Object.keys(hashedDataset).forEach(function (key) {
                var groupedRow = [];

                hashedDataset[key].forEach(function (row) {
                    Object.keys(columns).forEach(function (columnName) {
                        var aggType = columns[columnName]["aggType"],
                            index = columns[columnName]["index"],
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

            rows = _prepareRows(groupedDataset);

            return errors.length > 0 ? { error : errors.join("\n") } : {};
        };

        var _partition = function (data) {
            var hashedDataset = _hashDatasetByKeyColumns(data, true);

            if ("error" in hashedDataset) {
                return { "error" : hashedDataset.error };
            } else {
                hashedDataset = hashedDataset.hash;
            }

            partitionedBy = data.keyColumns;

            var columnsRow = Object.keys(columns).map(function (columnName) {
                return columns[columnName];
            }).sort(function (a, b) {
                return _numSort(a["index"], b["index"]);
            });

            Object.keys(hashedDataset).forEach(function (key) {
                var dataset = hashedDataset[key];
                partitionedRows[key] = dataset;
            });

            return {};
        };

        var _getPartitionKeys = function (data) {
            var keys =  Object.keys(partitionedRows).map(function (key) {
                return key.split("|");
            });

            return { keys : keys };
        };

        var _getPartitioned = function (data) {
            return { rows : _getVisibleRows(undefined, partitionedRows[data.key]) };
        };

        var _getDataset = function (data) {
            return { rows : _getVisibleRows(data.columnNames) };
        };

        var _getDistinctConsecutiveRows = function (data) {
            var visibleRows = _getVisibleRows([ data.columnName ]),
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

        var _getNumberOfRecords = function (data) {
            return { numRows : _getVisibleRows().length };
        };

        var _paginate = function (data) {
            rowsPerPage = data.rowsPerPage;

            return {};
        };

        var _getLastPage = function (data) {
            var visibleRows = _getVisibleRows(data.columnNames);

            return {
                visibleRows: visibleRows,
                lastPage: (Math.ceil(visibleRows.length / rowsPerPage) - 1)
            };
        };

        var _getPage = function (data) {
            var rowData = _getLastPage(data),
                start, end;

            data.lastPage = rowData.lastPage;

            _setPage(data);

            start = rowsPerPage * currentPage;
            end = start + rowsPerPage;

            return { rows : rowData.visibleRows.slice(start, end), currentPage : currentPage + 1 };
        };

        var _setPage = function (data) {

            if (!("lastPage" in data)) {
                data.lastPage = _getLastPage(data).lastPage;
            }

            if (typeof(data.pageNum) !== "undefined") {
                currentPage = data.pageNum - 1;
            } else if (typeof(currentPage) === "undefined") {
                currentPage = 0;
            } else if (data.incrementPage) {
                currentPage++;
            } else if (data.decrementPage) {
                currentPage--;
            }

            if (currentPage < 0) currentPage = 0;
            if (currentPage > data.lastPage) currentPage = data.lastPage;

            return { currentPage: currentPage + 1 };
        };

        var _getNumberOfPages = function (data) {
            var rowData = _getLastPage(data);

            return { numberOfPages: rowData.lastPage + 1 };
        };

        var _hideColumns = function (data) {
            if ("columnNames" in data) {
                data.columnNames.forEach(function (column) {
                    if (column in columns) {
                        columns[column]["isVisible"] = false;
                    }
                });
            } else if ("columnNameRegex" in data) {
                Object.keys(columns).forEach(function (column) {
                    if (RegExp(data.columnNameRegex).test(column)) {
                        columns[column]["isVisible"] = false;
                    }
                });
            }

            return {};
        };

        var _showColumns = function (data) {
            if ("columnNames" in data) {
                data.columnNames.forEach(function (column) {
                    if (column in columns) {
                        columns[column]["isVisible"] = true;
                    }
                });
            } else if ("columnNameRegex" in data) {
                Object.keys(columns).forEach(function (column) {
                    if (RegExp(data.columnNameRegex).test(column)) {
                        columns[column]["isVisible"] = true;
                    }
                });
            }

            return {};
        };

        var _hideAllColumns = function (data) {
            Object.keys(columns).forEach(function (column) {
                columns[column]["isVisible"] = false;
            });

            return {};
        };

        var _showAllColumns = function (data) {
            Object.keys(columns).forEach(function (column) {
                columns[column]["isVisible"] = true;
            });

            return {};
        };

        var _getAllColumns = function (data) {
            return { columns : columns };
        };

        var _getColumns = function (data) {
            return { columns : _getVisibleColumns() };
        };

        var _getRows = function (data) {
            var start = data.start, end = data.end;

            if (typeof(end) !== "undefined") {
                end = parseInt(end) + 1;
            }

            return { rows : _getDataset(data).rows.slice(start, end) };
        };

        var _getHashedRows = function (data) {
            var columns = data.columnNames,
                rows    = _getRows(data).rows;

            if (!columns.length) {
                columns = _getColumns().columns;
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

        var _getSummaryRows = function (data) {
            return { summaryRows : _getVisibleRows(data.columnNames, summaryRows) };
        };

        var _setSummaryRows = function (data) {
            debugger;
            summaryRows = _prepareRows(data.summaryRows);
            return {};
        };

        var _getExpectedNumRows = function (data) {
            return { exNumRows : expectedNumRows };
        };

        var _clearDataset = function () {
            columns         = {};
            rows            = [];
            summaryRows     = [];
            partitionedRows = [];
            expectedNumRows = undefined;

            return {};
        };

        var _requestDataset = function (data) {
            _clearDataset();

            return _requestDatasetForAppend(data);
        };

        var _requestDatasetForAppend = function (data) {
            var requestMade = false;

            if (typeof(socket) !== "undefined") {
                socket.send(data.request);
                requestMade = true;
            }
            if (typeof(ajaxDatasource) !== "undefined") {
                _ajax(data.request);
                requestMade = true;
            }

            return requestMade ? {} : { error: "Could not request dataset; no datasource defined." };
        };

        var _finish = function () {
            if (typeof(onSocketClose) !== "undefined" && socket.readyState !== 0 && socket.readyState !== 3) {
                socket.send(onSocketClose);
            }
        };

        var _addChildRows = function (data) {
            var newRows = _prepareRows(data.rows),
                rowHash = _hashRowsByKeyColumns([ data.joinOn ], newRows, {}, true),
                joinIdx = columns[data.joinOn].index;

            rows.forEach(function (row, i) {
                var rowValues = row.row,
                    children   = rowHash[rowValues[joinIdx]];

                if (children) {
                    hasChildElements = true;
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

        var _postMessage = function (data) {
            if (socket.readyState !== 0 && socket.readyState !== 3) {
                socket.send(data.message);
            }

            return {};
        };

        handleMessage = function (e) {
            var data = e.data, reply = {};

            if (typeof(data) === "undefined") {
                return;
            }

            if (data.cmd === "initialize") {
                var waitToConnect = _initialize(data);

                if (waitToConnect) {
                    var wait = function () {
                        if (isWsReady) {
                            postMessage(reply);
                        } else {
                            setTimeout(wait, 500);
                        }
                    };

                    setTimeout(wait, 250);

                    return;
                }
            } else {
                switch (data.cmd) {
                    case "sort":
                        reply = _sort(data);
                        break;
                    case "setDecimalMarkCharacter":
                        reply = _setDecimalMarkCharacter(data);
                        break;
                    case "applyFilter":
                        reply = _applyFilter(data);
                        break;
                    case "clearFilters":
                        reply = _clearFilters(data);
                        break;
                    case "filter":
                        reply = _filter(data);
                        break;
                    case "search":
                        reply = _search(data);
                        break;
                    case "applyLimit":
                        reply = _applyLimit(data);
                        break;
                    case "limit":
                        reply = _limit(data);
                        break;
                    case "removeColumns":
                        reply = _removeColumns(data);
                        break;
                    case "prependColumnNames":
                        reply = _prependColumnNames(data);
                        break;
                    case "alterColumnName":
                        reply = _alterColumnName(data);
                        break;
                    case "alterColumnSortType":
                        reply = _alterColumnSortType(data);
                        break;
                    case "alterColumnAggType":
                        reply = _alterColumnAggType(data);
                        break;
                    case "alterColumnTitle":
                        reply = _alterColumnTitle(data);
                        break;
                    case "append":
                        reply = _append(data);
                        break;
                    case "hash":
                        reply = _hashDatasetByKeyColumns(data);
                        break;
                    case "join":
                        reply = _joinHashes(data);
                        break;
                    case "group":
                        reply = _group(data);
                        break;
                    case "partition":
                        reply = _partition(data);
                        break;
                    case "getPartitionKeys":
                        reply = _getPartitionKeys(data);
                        break;
                    case "getPartitioned":
                        reply = _getPartitioned(data);
                        break;
                    case "paginate":
                        reply = _paginate(data);
                        break;
                    case "getPage":
                        reply = _getPage(data);
                        break;
                    case "setPage":
                        reply = _setPage(data);
                        break;
                    case "hideColumns":
                        reply = _hideColumns(data);
                        break;
                    case "showColumns":
                        reply = _showColumns(data);
                        break;
                    case "hideAllColumns":
                        reply = _hideAllColumns(data);
                        break;
                    case "showAllColumns":
                        reply = _showAllColumns(data);
                        break;
                    case "getAllColumns":
                        reply = _getAllColumns(data);
                        break;
                    case "getColumns":
                        reply = _getColumns(data);
                        break;
                    case "getRows":
                        reply = _getRows(data);
                        break;
                    case "getHashedRows":
                        reply = _getHashedRows(data);
                        break;
                    case "getSummaryRows":
                        reply = _getSummaryRows(data);
                        break;
                    case "setSummaryRows":
                        reply = _setSummaryRows(data);
                        break;
                    case "getDataset":
                        reply = _getDataset(data);
                        break;
                    case "getDistinctConsecutiveRows":
                        reply = _getDistinctConsecutiveRows(data);
                        break;
                    case "getNumRows":
                        reply = _getNumberOfRecords(data);
                        break;
                    case "getExpectedNumRows":
                        reply = _getExpectedNumRows(data);
                        break;
                    case "getNumberOfPages":
                        reply = _getNumberOfPages(data);
                        break;
                    case "clearDataset":
                        reply = _clearDataset(data);
                        break;
                    case "requestDataset":
                        reply = _requestDataset(data);
                        break;
                    case "requestDatasetForAppend":
                        reply = _requestDatasetForAppend(data);
                        break;
                    case "refresh":
                        reply["columns"] = _getVisibleColumns();
                        reply["rows"]    = _getVisibleRows();
                        break;
                    case "refreshAll":
                        reply["columns"] = columns;
                        reply["rows"]    = rows.map(function (row) { return row.row; });
                        break;
                    case "finish":
                        _finish();
                        break;
                    case "addChildRows":
                        reply = _addChildRows(data);
                        break;
                    case "postMessage":
                        reply = _postMessage(data);
                        break;
                    default:
                        reply["error"] = "Unrecognized DataWorker command: " + data.cmd;
                }
            }

            postMessage(reply);
        };

    }

})();
