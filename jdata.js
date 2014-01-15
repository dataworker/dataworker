(function() {
    "use strict";

    var srcPath = getSourcePath();

    function getSourcePath () {
        var scripts = document.getElementsByTagName('script'),
            srcFile = scripts[scripts.length - 1].src;

        return srcFile.replace(
            /(http:\/\/)?.*?(\/(.*\/)?).*/,
            function () { return arguments[2]; }
        );
    }

    var JData = window.JData = function JData (dataset) {
        var self = this instanceof JData ? this : Object.create(JData.prototype);

        self._columns       = {};
        self._rows          = [];
        self._distinct_rows = [];
        self._hash          = {};

        self._num_rows = 0;
        self._expected_num_rows = 0;
        self._current_page = undefined;
        self._number_of_pages = undefined;

        self._partitioned_datasets = {};

        self._render_function = function () {};

        self._action_queue = new ActionQueue();

        self._initialize_callbacks(dataset);
        self._initialize_web_worker(dataset);

        window.addEventListener("beforeunload", function () {
            self._worker.postMessage({ cmd : "finish" });
        });

        return self;
    };

    JData.prototype._queue_next = function (action) {
        var self = this;

        self._action_queue.queueNext(action);

        return self;
    };

    JData.prototype._finish_action = function () {
        var self = this;

        self._action_queue.finishAction();

        return self;
    };

    JData.prototype._initialize_callbacks = function (dataset) {
        var self = this;

        self._on_receive_columns_tracker = false;
        if ("on_receive_columns" in dataset) {
            self._on_receive_columns = function () {
                self._on_receive_columns_tracker = false;
                dataset["on_receive_columns"].apply(this, arguments);
            };
        } else {
            self._on_receive_columns = function () {};
        }

        self._on_all_rows_received_tracker = false;
        if ("on_all_rows_received" in dataset) {
            self._on_all_rows_received = function () {
                self._on_all_rows_received_tracker = false;
                dataset["on_all_rows_received"].apply(this, arguments);
            };
        } else {
            self._on_all_rows_received = function () {};
        }

        self._on_receive_rows = "on_receive_rows" in dataset
                              ? dataset["on_receive_rows"]
                              : function () {};

        self._on_trigger = "on_trigger" in dataset
                         ? dataset["on_trigger"]
                         : function () {};

        self._on_error = "on_error" in dataset ? dataset["on_error"] : function () {};

        return self;
    };

    JData.prototype._initialize_web_worker = function (dataset) {
        var self = this, columns, rows, datasource, authenticate, request;

        if (dataset instanceof Array) {
            columns = dataset.slice(0, 1)[0];
            rows    = dataset.slice(1);
        } else {
            datasource   = dataset.datasource;

            authenticate = (typeof dataset.authenticate === "string")
                ? dataset.authenticate
                : JSON.stringify(dataset.authenticate);

            request      = (typeof dataset.request === "string")
                ? dataset.request
                : JSON.stringify(dataset.request);
        }

        self._queue_next(function () {
            var this_action_queue = this;

            self._worker = ( typeof Worker === "undefined" )
                ? ( new JDataWorker() )
                : ( new Worker(srcPath + "jdata_worker.js") );

            self._worker.onmessage = function (e) {
                if ("rows_received" in e.data) {
                    self._on_receive_rows(e.data.rows_received);
                    return;
                }
                if ("trigger_msg" in e.data) {
                    self._on_trigger(e.data.trigger_msg);
                    return;
                }
                if ("all_rows_received" in e.data) {
                    self._on_all_rows_received_tracker = true;
                    self._on_all_rows_received();
                    return;
                }

                if ("current_page" in e.data) {
                    self._current_page = e.data.current_page;
                }

                if ("number_of_pages" in e.data) {
                    self._number_of_pages = e.data.number_of_pages;
                }

                if ("error"         in e.data) self._on_error(e.data.error);

                if ("columns"       in e.data) self._columns = e.data.columns;
                if ("rows"          in e.data) self._rows = e.data.rows;
                if ("distinct_rows" in e.data) self._distinct_rows = e.data.distinct_rows;

                if ("hash"          in e.data) self._hash = e.data.hash;
                if ("keys"          in e.data) self._keys = e.data.keys;

                if ("num_rows"      in e.data) self._num_rows = e.data.num_rows;
                if ("ex_num_rows"   in e.data) self._expected_num_rows = e.data.ex_num_rows;

                if ("columns_received" in e.data) {
                    self._on_receive_columns_tracker = true;
                    self._on_receive_columns(self._columns, self._expected_num_rows);
                    return;
                }

                self._finish_action();
            };

            self._worker.postMessage({
                cmd          : "initialize",
                columns      : columns,
                rows         : rows,
                datasource   : datasource,
                authenticate : authenticate,
                request      : request,
                on_close     : dataset.on_close
            });
        });

        return self;
    };

    JData.prototype.finish = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "finish" });
        })._queue_next(function () {
            self._worker.terminate();
        });

        return self;
    };

    JData.prototype._get_columns = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "get_columns" });
        });

        return self;
    };

    JData.prototype.get_columns = function (callback) {
        var self = this;

        self._get_columns()._queue_next(function () {
            callback(self._columns);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype._refresh = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "refresh" });
        });

        return self;
    };

    JData.prototype._refresh_all = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "refresh_all" });
        });

        return self;
    };

    JData.prototype.get_dataset = function () {
        var self = this,
            callback = arguments[0],
            column_names = arguments[1] instanceof Array
                         ? arguments[1]
                         : Array.prototype.slice.call(arguments, 1);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd          : "get_dataset",
                column_names : column_names
            });
        })._queue_next(function () {
            callback(self._rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.get_distinct_consecutive_rows = function () {
        var self = this,
            callback = arguments[0],
            column_name = arguments[1];

        self._queue_next(function () {
            self._worker.postMessage({
                cmd         : "get_distinct_consecutive_rows",
                column_name : column_name
            });
        })._queue_next(function () {
            callback(self._distinct_rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.apply_filter = function () {
        var self = this,
            filters = arguments[0] instanceof Array
                    ? arguments[0]
                    : Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd     : "apply_filter",
                filters : filters
            });
        });

        return self;
    };

    JData.prototype.clear_filters = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd : "clear_filters"
            });
        });

        return self;
    };

    JData.prototype.filter = function () {
        var self = this,
            regex = arguments[0],
            relevant_columns = arguments[1] instanceof Array
                             ? arguments[1]
                             : Array.prototype.slice.call(arguments, 1);
        
        self._queue_next(function () {
            self._worker.postMessage({
                cmd              : "filter",
                regex            : regex,
                relevant_columns : relevant_columns
            });
        });

        return self;
    };

    JData.prototype.apply_limit = function (num_rows) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd      : "apply_limit",
                num_rows : num_rows
            });
        });

        return self;
    };

    JData.prototype.limit = function (num_rows) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd      : "limit",
                num_rows : num_rows
            });
        });

        return self;
    };

    JData.prototype.sort = function () {
        var self = this, sort_columns = arguments[0] instanceof Array
                                      ? arguments[0]
                                      : Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd     : "sort",
                sort_on : sort_columns,
                columns : self._columns,
                rows    : self._rows
            });
        });

        return self;
    };

    JData.prototype.set_decimal_mark_character = function (decimal_mark_character) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd                    : "set_decimal_mark_character",
                decimal_mark_character : decimal_mark_character
            });
        });

        return self;
    };

    JData.prototype.remove_columns = function () {
        var self = this,
            columns_to_remove = arguments[0] instanceof Array
                              ? arguments[0]
                              : Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd               : "remove_columns",
                columns_to_remove : columns_to_remove
            });
        });

        return self;
    };

    JData.prototype.paginate = function (rows_per_page) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd           : "paginate",
                rows_per_page : rows_per_page
            });
        });

        return self;
    };

    JData.prototype.get_next_page = function (callback) {
        var self = this;

        self._get_page(undefined, true, false)._queue_next(function() {
            callback(self._rows, self._current_page);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.get_previous_page = function (callback) {
        var self = this;

        self._get_page(undefined, false, true)._queue_next(function () {
            callback(self._rows, self._current_page);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.get_number_of_pages = function (callback) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd : "get_number_of_pages"
            });
        })._queue_next(function () {
            callback(self._number_of_pages);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype._get_page = function (page_num, increment_page, decrement_page) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd            : "get_page",
                page_num       : page_num,
                increment_page : increment_page,
                decrement_page : decrement_page
            });
        });

        return self;
    };

    JData.prototype.get_page = function (callback, page_num) {
        var self = this;

        self._get_page(page_num)._queue_next(function () {
            callback(self._rows, self._current_page);
            self._finish_action();
        });

        return self;
    };

    JData.prototype.set_page = function (page_num) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                    cmd      : "set_page",
                    page_num : page_num
            });
        });

        return self;
    };

    JData.prototype.get_rows = function (callback, start, end) {
        var self         = this,
            column_names = arguments[3] instanceof Array
                         ? arguments[3]
                         : Array.prototype.slice.call(arguments, 3);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd          : "get_rows",
                start        : start,
                end          : end,
                column_names : column_names
            });
        })._queue_next(function () {
            callback(self._rows);
            self._finish_action();
        });

        return self;
    };

    JData.prototype.get_columns_and_records = function (callback) {
        var self = this;

        self._refresh()._queue_next(function () {
            callback(self._columns, self._rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.get_all_columns_and_all_records = function (callback) {
        var self = this;

        self._refresh_all()._queue_next(function () {
            callback(self._columns, self._rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.append = function (data) {
        var self = this;

        self._queue_next(function () {
            if (data instanceof JData) {
                data.get_all_columns_and_all_records(function (new_columns, new_rows) {
                    self._worker.postMessage({
                        cmd         : "append",
                        new_columns : new_columns,
                        new_rows    : new_rows
                    });
                });
            } else {
                self._worker.postMessage({
                    cmd         : "append",
                    new_columns : data.slice(0, 1)[0],
                    new_rows    : data.slice(1)
                });
            }
        });

        return self;
    };

    JData.prototype.join = function (fdata, pk, fk, join_type) {
        var self = this, args = Array.prototype.slice.call(arguments);
        var f_hash, f_columns;

        self._queue_next(function () {
            if (!pk.length || !fk.length) {
                self._on_error("No join key(s) provided.");
            }
            if (pk.length != fk.length) {
                self._on_error("Odd number of join keys.");
            }
            if (
                typeof(join_type) !== "undefined"
                && !(join_type === "left" || join_type === "right" || join_type === "inner")
            ) {
                self._on_error("Unknown join type.");
            }

            return self._finish_action();
        });


        self._queue_next(function () {
            self._worker.postMessage({ cmd : "get_all_columns" });
        })._queue_next(function () {
            fdata.get_all_columns(function (columns) {
                Object.keys(columns).forEach(function (column_name) {
                    if (column_name in self._columns) {
                        self._on_error("Column names overlap.");
                    }
                });

                f_columns = columns;

                return self._finish_action();
            });
        })._queue_next(function () {
            fdata.get_hash_of_dataset_by_key_columns.call(fdata, function (hash) {
                f_hash = hash;
                return self._finish_action();
            }, fk);
        })._queue_next(function () {
            self._worker.postMessage({
                cmd         : "join",
                key_columns : pk,
                r_hash      : f_hash,
                join_type   : join_type,
                f_columns   : f_columns
            });
        });

        return self;
    };

    JData.prototype.get_hash_of_dataset_by_key_columns = function (callback, key_columns) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd         : "hash",
                key_columns : key_columns
            });
        })._queue_next(function () {
            callback(self._hash);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.prepend_column_names = function (prepend) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd     : "prepend_column_names",
                prepend : prepend
            });
        });

        return self;
    };

    JData.prototype.alter_column_name = function (old_name, new_name) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd      : "alter_column_name",
                old_name : old_name,
                new_name : new_name
            });
        });

        return self;
    };

    JData.prototype.alter_column_sort_type = function (column, sort_type) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd       : "alter_column_sort_type",
                column    : column,
                sort_type : sort_type
            });
        });

        return self;
    };

    JData.prototype.alter_column_aggregate_type = function (column, agg_type) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd      : "alter_column_agg_type",
                column   : column,
                agg_type : agg_type
            });
        });

        return self;
    };

    JData.prototype.alter_column_title = function (column, title) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({
                cmd    : "alter_column_title",
                column : column,
                title  : title
            });
        });

        return self;
    };

    JData.prototype.group = function () {
        var self = this,
            group_by = arguments[0] instanceof Array
                     ? arguments[0]
                     : Array.prototype.slice.call(arguments);
        
        self._queue_next(function () {
            self._worker.postMessage({
                cmd            : "group",
                key_columns    : group_by
            });
        });

        return self;
    };

    JData.prototype.partition = function () {
        var self = this,
            partition_by = arguments[0] instanceof Array
                         ? arguments[0]
                         : Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd         : "partition",
                key_columns : partition_by
            });
        });

        return self;
    };

    JData.prototype.get_partition_keys = function (callback) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "get_partition_keys" });
        })._queue_next(function () {
            callback(self._keys);
            self._finish_action();
        });

        return self;
    };

    JData.prototype.get_partitioned = function () {
        var self = this,
            callback = arguments[0],
            keys = arguments[1] instanceof Array
                 ? arguments[1]
                 : Array.prototype.slice.call(arguments, 1);

        self._queue_next(function () {
            self._worker.postMessage({
                cmd : "get_partitioned",
                key : keys.join("|")
            });
        })._queue_next(function () {
            callback(self._rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.render = function (render_function) {
        var self = this;

        if (typeof(render_function) === "function") {
            self._queue_next(function () {
                self._render_function = render_function;
                return self._finish_action();
            });
        } else {
            self._queue_next(function () {
                self._render_function();
                return self._finish_action();
            });
        }

        return self;
    };

    JData.prototype.on_error = function (cb, actImmediately) {
        var self = this;

        if (actImmediately) {
            self._on_error = cb;
        } else {
            self._queue_next(function () {
                self._on_error = cb;

                return self._finish_action();
            });
        }

        return self;
    };

    JData.prototype.on_receive_columns = function (callback, actImmediately) {
        var self = this;

        var wrappedCallback = function () {
            self._on_receive_columns_tracker = false;
            callback.apply(this, arguments);
        };

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._on_receive_columns = wrappedCallback;

                if (self._on_receive_columns_tracker) {
                    callback(self._columns, self._expected_num_rows);
                }
            } else {
                self._queue_next(function () {
                    self._on_receive_columns = wrappedCallback;

                    if (self._on_receive_columns_tracker) {
                        callback(self._columns, self._expected_num_rows);
                    }

                    return self._finish_action();
                });
            }
        }

        return self;
    };

    JData.prototype.on_receive_rows = function (callback, actImmediately) {
        var self = this;

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._on_receive_rows = callback;
            } else {
                self._queue_next(function () {
                    self._on_receive_rows = callback;
                    return self._finish_action();
                });
            }
        }

        return self;
    };

    JData.prototype.on_trigger = function (callback, actImmediately) {
        var self = this;

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._on_trigger = callback;
            } else {
                self._queue_next(function () {
                    self._on_trigger = callback;
                    return self._finish_action();
                });
            }
        }

        return self;
    };

    JData.prototype.on_all_rows_received = function (callback, actImmediately) {
        var self = this;

        var wrappedCallback = function () {
            self._on_all_rows_received_tracker = false;
            callback.apply(this, arguments);
        };

        if (typeof(callback) === "function") {
            if (actImmediately) {
                self._on_all_rows_received = wrappedCallback;

                if (self._on_all_rows_received_tracker) {
                    callback();
                }
            } else {
                self._queue_next(function () {
                    self._on_all_rows_received = wrappedCallback;

                    if (self._on_all_rows_received_tracker) {
                        callback();
                    }

                    return self._finish_action();
                });
            }
        }

        return self;
    };

    JData.prototype.get_number_of_records = function (callback) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "get_num_rows" });
        })._queue_next(function () {
            callback(self._num_rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.clone = function (callback) {
        var self = this;

        self.get_all_columns_and_all_records(function (columns, records) {
            var columns_row = Object.keys(columns).map(function (column_name) {
                return columns[column_name];
            }).sort(function (a, b) {
                return a["index"] - b["index"];
            });

            var dataset = [ columns_row ].concat(records);

            callback(new JData(dataset));
        });

        return self;
    };

    JData.prototype.get_expected_number_of_records = function (callback) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "get_expected_num_rows" });
        })._queue_next(function () {
            callback(self._expected_num_rows);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.request_dataset = function (request) {
        var self = this;

        self._queue_next(function () {
            self._on_all_rows_received_tracker = false;
            self._on_receive_columns_tracker = false;

            self._worker.postMessage({
                cmd     : "request_dataset",
                request : request
            });
        });

        return self;
    };


    JData.prototype.request_dataset_for_append = function (request) {
        var self = this;

        self._queue_next(function () {
            self._on_all_rows_received_tracker = false;
            self._on_receive_columns_tracker = false;

            self._worker.postMessage({
                cmd     : "request_dataset_for_append",
                request : request
            });
        });

        return self;
    };

    JData.prototype.hide_columns = function () {
        var self = this, msg = { cmd : "hide_columns" },
            column_names = arguments[0] instanceof Array
                         ? arguments[0]
                         : Array.prototype.slice.call(arguments);

        if (column_names[0] instanceof RegExp) {
            msg["column_name_regex"] = column_names[0];
        } else {
            msg["column_names"] = column_names;
        }

        self._queue_next(function () {
            self._worker.postMessage(msg);
        });

        return self;
    };

    JData.prototype.show_columns = function () {
        var self = this, msg = { cmd : "show_columns" },
            column_names = arguments[0] instanceof Array
                         ? arguments[0]
                         : Array.prototype.slice.call(arguments);

        if (column_names[0] instanceof RegExp) {
            msg["column_name_regex"] = column_names[0];
        } else {
            msg["column_names"] = column_names;
        }

        self._queue_next(function () {
            self._worker.postMessage(msg);
        });

        return self;
    };

    JData.prototype.hide_all_columns = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "hide_all_columns" });
        });

        return self;
    };

    JData.prototype.show_all_columns = function () {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "show_all_columns" });
        });

        return self;
    };

    JData.prototype.get_all_columns = function (callback) {
        var self = this;

        self._queue_next(function () {
            self._worker.postMessage({ cmd : "get_all_columns" });
        })._queue_next(function () {
            callback(self._columns);
            return self._finish_action();
        });

        return self;
    };

    JData.prototype.add_child_rows = function (data, join_column) {
        var self = this;

        self._queue_next(function () {
            var callback = function (rows) {
                self._worker.postMessage({
                    cmd     : "add_child_rows",
                    join_on : join_column,
                    rows    : rows
                });
            };

            if (data instanceof JData) {
                data.get_dataset(function (rows) {
                    callback(rows);
                });
            } else {
                callback(data);
            }
        });

        return self;
    };

    JData.prototype.then = function (callback) {
        var self = this;

        self._queue_next(function () {
            callback();
            return self._finish_action();
        });

        return self;
    };
})();
