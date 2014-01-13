(function () {
"use strict";

var handle_message;

if (typeof window === "undefined") {
    run_jdata_worker(self);
    self.addEventListener("message", handle_message, false);
} else {
    var JDataWorker = window.JDataWorker = function JDataWorker () {
        var self = this instanceof JDataWorker ? this : Object.create(JDataWorker.prototype);

        try {
            run_jdata_worker(self);
        } catch (error) {
            self.onerror(error);
        }

        return self;
    };

    JDataWorker.prototype.postMessage = function (message, transferList) {
        var self = this;

        setTimeout(function () {
            try {
                handle_message({ data : message });
            } catch (error) {
                self.onerror(error);
            }
        }, 0);

        return self;
    };

    JDataWorker.prototype.onmessage = function (e) { return this; };

    JDataWorker.prototype.onerror = function (e) {
        if (typeof console !== "undefined") console.error(e);
        return this;
    };

    JDataWorker.prototype.terminate = function () { delete this; };
}

function run_jdata_worker(self) {

var columns = {}, rows = [],
    partitioned_by = [], partitioned_rows = {},
    socket, on_socket_close, is_ws_ready, expected_num_rows,
    ajax_datasource,
    rows_per_page = 10, current_page = undefined,
    only_valid_for_numbers_regex = /[^0-9.\-]/g,
    authentication,
    has_child_elements = false,
    postMessage;

if (typeof window === "undefined") {
    postMessage = function () { return self.postMessage.apply(self, arguments); };
} else {
    postMessage = function (message) {
        return self.onmessage({ data: message });
    };
}

var _prepare_columns = function (raw_columns) {
    var prepared_columns = {};

    raw_columns.forEach(function (column, i) {
        if (typeof(column) === "string") {
            column = { name: column };
        }

        column.sort_type  = column.sort_type || "alpha";
        column.agg_type   = column.agg_type  || "max";
        column.title      = column.title     || column.name;
        column.index      = i;
        column.is_visible = ("is_visible" in column) ? column.is_visible : true;

        prepared_columns[column.name] = column;
    });

    return prepared_columns;
};

var _prepare_rows = function (raw_rows) {
    return raw_rows.map(function (row) {
        return {
            row        : row,
            is_visible : true
        };
    });
};

var _strip_row_metadata = function (processed_rows) {
    return processed_rows.map(function (row) { return row["row"]; });
};

var _get_visible_columns = function () {
    var visible_columns = {}, index = 0;

    Object.keys(columns).sort(function (a, b) {
        return columns[a].index - columns[b].index;
    }).forEach(function (column_name) {
        if (columns[column_name].is_visible) {
            visible_columns[column_name] = JSON.parse(JSON.stringify(columns[column_name]));

            visible_columns[column_name].index = index++;
        }
    });

    return visible_columns;
};

var _get_visible_rows = function (requested_columns, requested_rows) {
    var visible_column_idxs = [], visible_rows = [];

    if (!(requested_columns || []).length) requested_columns = undefined;
    requested_rows = requested_rows || rows;

    if (has_child_elements) {
        var rows_with_children = [];
        requested_rows.forEach(function (row) {
            rows_with_children.push(row);
            if (row.has_children) {
                rows_with_children.push.apply(rows_with_children, row.children);
            }
        });

        requested_rows = rows_with_children;
    }

    (requested_columns || Object.keys(columns)).forEach(function (column_name) {
        var column = columns[column_name];

        if (requested_columns || column.is_visible) {
            visible_column_idxs.push(column.index);
        }
    });

    requested_rows.forEach(function (row) {
        if (row["is_visible"]) {
            var new_row = visible_column_idxs.map(function (idx) {
                return row["row"][idx];
            });

            new_row.parent_row = row.parent_row;

            visible_rows.push(new_row);
        }
    });

    return visible_rows;
};

var _initialize = function (data, use_backup) {
    var wait_to_connect = false;

    authentication = data.authenticate;

    try {
        if (use_backup) throw new Error();

        if (typeof(data.datasource) === "undefined") {
            columns = _prepare_columns(data.columns);
            rows    = _prepare_rows(data.rows);
        } else if (data.datasource.match(/^https?:\/\//)) {
            ajax_datasource = data.datasource;

            if (typeof(data.request) !== "undefined") {
                _request_dataset(data);
            }
        } else if (data.datasource.match(/^wss?:\/\//)) {
            wait_to_connect = _initialize_websocket_connection(data);
        } else {
            postMessage({
                error: "Error: Could not initialize JData; unrecognized datasource."
            });
        }
    } catch (error) {
        if (data.backup_datasource) {
            data.datasource = data.backup_datasource;
            delete data.backup_datasource;

            wait_to_connect = _initialize(data);
        } else {
            postMessage({
                error : "Error: " + error
            });
        }
    }

    return wait_to_connect;
};

var _get_request_params = function (params) {
    var query_string = "";

    try {
        if (params) {
            if (typeof params === "string") {
                params = JSON.parse(params);
            }

            for (var key in params) {
                query_string += key + "=" + encodeURIComponent(params[key]) + "&";
            }
        }
    } catch (e) {
        if (typeof params === "string") {
            query_string += params;
        }
    }

    return query_string;
}

var _ajax = function (request) {
    var xml_http = new XMLHttpRequest(),
        url = ajax_datasource + (request.match(/^\?/) ? "" : "?");

    url += _get_request_params(request);
    url += _get_request_params(authentication);

    xml_http.onreadystatechange = function () {
        if (xml_http.readyState == 4 && xml_http.status == 200) {
            var msg = JSON.parse(xml_http.responseText);

            if (msg.error) {
                postMessage({ error : msg.error });
            }

            if (msg.columns) {
                columns = _prepare_columns(msg.columns);
            }
            if (msg.rows) {
                if (typeof(rows) === "undefined") rows = [];
                rows = rows.concat(_prepare_rows(msg.rows));
            }

            postMessage({
                columns_received : true,
                columns          : _get_visible_columns(),
                ex_num_rows      : _get_visible_rows().length
            });

            postMessage({ all_rows_received : true });
        }
    };

    xml_http.open("GET", url, true);
    xml_http.send();
};

var _initialize_websocket_connection = function (data) {
    socket = new WebSocket(data.datasource);
    is_ws_ready = false;

    socket.onopen  = function () {
        if (data.authenticate) {
            socket.send(data.authenticate);
        }

        if (typeof(data.request) !== "undefined") {
            socket.send(data.request);
        }

        is_ws_ready = true;
    };
    socket.onclose = function () {};
    socket.onerror = function (error) {
        if (error.target.readyState === 0 || error.target.readyState === 3) {
            is_ws_ready = true;
            socket = null;

            _initialize(data, true);
        } else {
            postMessage({
                error : "Error: Problem with connection to datasource."
            });
        }
    };

    socket.onmessage = function (msg) {
        msg = JSON.parse(msg.data);

        if (msg.error) {
            postMessage({ error : msg.error });
            return;
        }

        if (msg.columns) {
            columns = _prepare_columns(msg.columns);
        }

        if (msg.expected_num_rows === "INFINITE") {
            expected_num_rows = msg.expected_num_rows;
        } else if (typeof(msg.expected_num_rows) !== "undefined") {
            if (typeof(expected_num_rows) === "undefined") {
                expected_num_rows = parseInt(msg.expected_num_rows);
            } else {
                expected_num_rows += parseInt(msg.expected_num_rows);
            }
        }

        if (msg.rows) {
            var prepared_rows = _prepare_rows(msg.rows);

            if (typeof(rows) === "undefined") rows = [];
            rows.push.apply(rows, prepared_rows);

            if (partitioned_by.length > 0) {
                _insert_into_partitioned_rows(prepared_rows);
            }

            if (expected_num_rows !== "INFINITE" && rows.length == expected_num_rows) {
                postMessage({ all_rows_received : true });
            } else {
                postMessage({ rows_received : msg.rows.length });
            }
        } else if (
            typeof(columns) !== "undefined"
            && typeof(msg.expected_num_rows) !== "undefined"
            && expected_num_rows != rows.length
        ) {
            postMessage({
                columns_received : true,
                columns          : _get_visible_columns(),
                ex_num_rows      : expected_num_rows
            });
        }

        if (msg.expected_num_rows == 0) postMessage({ all_rows_received : true });

        if (msg.trigger) postMessage({ trigger_msg : msg.msg });

        on_socket_close = data.on_close;
    };

    return true;
};

var _insert_into_partitioned_rows = function (prepared_rows) {
    _hash_rows_by_key_columns(
        partitioned_by,
        prepared_rows,
        partitioned_rows,
        true
    );
};

var _alpha_sort = function (a, b) {
    a = (a || '').toLowerCase();
    b = (b || '').toLowerCase();

    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
};

var _locale_alpha_sort = function (a, b) {
    a = (a || '').toLowerCase();
    b = (b || '').toLowerCase();

    return a.localeCompare(b);
};

var _num_sort = function (a, b) {
    a += "";
    a = a.replace(only_valid_for_numbers_regex, '');
    a = parseFloat(a);

    b += "";
    b = b.replace(only_valid_for_numbers_regex, '');
    b = parseFloat(b);

    if (!isNaN(a) && !isNaN(b))
        return a - b;
    else if (isNaN(a) && isNaN(b))
        return 0;
    else
        return isNaN(a) ? -1 : 1;
};

var _sort = function (data) {
    var sort_on = data.sort_on;

    function _compare_rows(a, b) {
        var i, sort_column, column_name, reverse, sort_type, sort_result, val_a, val_b;

        for (i = 0; i < sort_on.length; i++) {
            sort_column = sort_on[i].match(/(-?)(\w+)/);
            column_name = sort_column[2];
            reverse     = !!sort_column[1];
            sort_type   = columns[column_name]["sort_type"];

            if (reverse) {
                val_b = a["row"][columns[column_name]["index"]];
                val_a = b["row"][columns[column_name]["index"]];
            } else {
                val_a = a["row"][columns[column_name]["index"]];
                val_b = b["row"][columns[column_name]["index"]];
            }

            if (sort_type === "alpha") {
                sort_result = _alpha_sort(val_a, val_b);
            } else if (sort_type === "locale_alpha") {
                sort_result = _locale_alpha_sort(val_a, val_b);
            } else if (sort_type === "num") {
                sort_result = _num_sort(val_a, val_b);
            } else {
                throw new Error("Unknown sort type.");
            }

            if (sort_result !== 0) {
                return sort_result;
            }
        }

        return 0;
    }

    rows.sort(_compare_rows);

    if (has_child_elements) {
        rows.forEach(function (row) {
            if (row.has_children) {
                row.children.sort(_compare_rows);
            }
        });
    }

    return {};
};

var _set_decimal_mark_character = function (data) {
    var decimal_mark_character = data.decimal_mark_character;

    only_valid_for_numbers_regex = new RegExp("[0-9\-" + decimal_mark_character + "]", "g");

    return {};
};

var _apply_filter = function (data) {
    var filters = data.filters, i = 0, column,
        is_simple_filter = !("column" in filters[0]);

    if (is_simple_filter) {
        var regex = filters[0],
            relevant_columns = filters.slice(1),
            relevant_indexes = relevant_columns.map(function (column) {
                return columns[column]["index"];
            });

        rows.forEach(function (row) {
            if (!row["is_visible"]) return;

            row["is_visible"] = false;

            for (i = 0; i < row["row"].length; i++) {
                column = row["row"][i];

                if (
                    (relevant_indexes.length === 0 || relevant_indexes.indexOf(i) !== -1)
                    && column && column.match(regex)
                ) {
                    row["is_visible"] = true;
                    break;
                }
            }
        });
    } else {
        rows.forEach(function (row) {
            if (!row["is_visible"]) return;

            row["is_visible"] = true;

            for (i = 0; i < filters.length; i++) {
                var filter = filters[i], column_index, column;

                if (filter.column in columns) {
                    column_index = columns[filter.column]["index"],
                    column = row["row"][column_index];

                    if (column && column.match(filter.regex)) {
                        row["is_visible"] = true;
                    } else {
                        row["is_visible"] = false;
                        break;
                    }
                } else {
                    break;
                }
            };
        });
    }

    return {};
};

var _clear_filters = function () {
    rows.forEach(function (row) {
        row["is_visible"] = true;
    });

    return {};
};

var _filter = function (data) {
    var regex = data.regex, relevant_columns = data.relevant_columns;
    var i, column, filtered_dataset = [],
        relevant_indexes = relevant_columns.map(function (column) {
            return columns[column]["index"];
        });

    rows.forEach(function (row) {
        for (i = 0; i < row["row"].length; i++) {
            column = row["row"][i];

            if (
                (relevant_indexes.length === 0 || relevant_indexes.indexOf(i) !== -1)
                && column !== null && column.match(regex)
            ) {
                filtered_dataset.push(row);
                break;
            }
        }
    });

    rows = filtered_dataset;

    return {};
};

var _apply_limit = function (data) {
    var num_rows = data.num_rows;
    var i = 0;

    rows.forEach(function (row) {
        if (i++ < num_rows) {
            row["is_visible"] = true;
        } else {
            row["is_visible"] = false;
        }
    });

    return {};
};

var _limit = function (data) {
    var num_rows = data.num_rows;

    rows = rows.slice(0, num_rows);

    return {};
};

var _remove_columns = function (data) {
    var columns_to_remove = data.columns_to_remove;
    var i = 0, columns_to_keep = {}, filtered_dataset = [];

    Object.keys(columns).forEach(function (column_name, i) {
        if (columns_to_remove.indexOf(column_name) === -1) {
            var column = columns[column_name];
            column["index"] = i;

            columns_to_keep[column_name] = column;
        }
    });

    rows.forEach(function (row) {
        var filtered_row = [];

        Object.keys(columns_to_keep).forEach(function (column_name) {
            filtered_row[columns_to_keep[column_name]["index"]]
                = row["row"][columns[column_name]["index"]];
        });

        filtered_dataset.push(filtered_row);
    });

    columns = columns_to_keep;
    rows    = _prepare_rows(filtered_dataset);

    return {};
};

var _prepend_column_names = function (data) {
    var prepend = data.prepend;
    var new_columns = {};

    Object.keys(columns).forEach(function (column_name) {
        new_columns[prepend + column_name] = columns[column_name];
    });

    columns = new_columns;

    return {};
};

var _alter_column_name = function (data) {
    var old_name = data.old_name, new_name = data.new_name;
    var error;

    Object.keys(columns).forEach(function (column_name) {
        if (column_name !== old_name && column_name === new_name) {
            error = "Column " + new_name + " already exists in the dataset.";
        }
    });

    columns[new_name] = columns[old_name];
    delete columns[old_name];

    return typeof(error) === "undefined" ? {} : { error : error };
};

var _alter_column_sort_type = function (data) {
    var column = data.column, sort_type = data.sort_type;

    columns[column]["sort_type"] = sort_type;

    return {};
};

var _alter_column_agg_type = function (data) {
    var column = data.column, agg_type = data.agg_type;

    columns[column]["agg_type"] = agg_type;

    return {};
};

var _alter_column_title = function (data) {
    var column = data.column, title = data.title;

    columns[column]["title"] = title;

    return {};
};

var _extract_column_names_in_order = function (columns) {
    return Object.keys(columns).sort(function (a, b) {
        return _num_sort(
            columns[a]["index"],
            columns[b]["index"]
        );
    });
};

var _check_columns_for_append = function (new_columns) {
    var i, error_msg;

    if (new_columns instanceof Array) {
        new_columns = _prepare_columns(new_columns);
    }

    error_msg = function () {
        var original_columns = _extract_column_names_in_order(columns) .join(", ");
        var append_columns = _extract_column_names_in_order(new_columns) .join(", ");

        return "Cannot append dataset (columns do not match):\n\t"
               + original_columns + "\n\t\tVS\n\t" + append_columns;
    };

    if (Object.keys(new_columns).length === Object.keys(columns).length) {
        var column_names = Object.keys(new_columns);

        for (i = 0; i < column_names.length; i++) {
            var name = column_names[i];

            if (typeof(columns[name]) === "undefined") {
                return error_msg();
            } else if (new_columns[name]["index"] !== columns[name]["index"]) {
                return error_msg();
            }
        }
    } else {
        return error_msg();
    }
};

var _append = function (data) {
    if (data.new_columns) {
        var error = _check_columns_for_append(data.new_columns);
        if (error) return { error: error };
    }

    rows.splice.apply(rows,
        [ "index" in data ? data.index : rows.length, 0 ].concat(
            data.prepared_rows || _prepare_rows(data.new_rows)
        )
    );

    return {};
};

var _hash_rows_by_key_columns = function (key_columns, my_rows, hash, prepared_rows) {
    var key_indexes = key_columns.map(function (column_name) {
        return columns[column_name]["index"];
    });

    my_rows.forEach(function (row) {
        var row_values = row["row"],
            key = key_indexes.map(function (i) { return row_values[i]; }).join("|");

        if (key in hash) {
            hash[key].push(prepared_rows ? row : row_values);
        } else {
            hash[key] = [ prepared_rows ? row : row_values ];
        }
    });

    return hash;
};

var _hash_dataset_by_key_columns = function (data, prepared_rows) {
    var key_columns = data.key_columns;
    var key_indexes, hash = {}, errors = [], reply = {};

    key_columns = key_columns instanceof Array ? key_columns : [ key_columns ];
    key_columns.forEach(function (column) {
        if (!column in columns) {
            errors.push("Column '" + column + "' not in dataset.");
        }
    });

    _hash_rows_by_key_columns(key_columns, rows, hash, prepared_rows);

    reply["hash"] = hash;
    if (errors.length) reply["error"] = errors.join("\n");

    return reply;
};

var _join_hashes = function (data) {
    var l_hash = _hash_dataset_by_key_columns(data), r_hash = data.r_hash,
        join_type = data.join_type, f_columns = data.f_columns;

    if ("error" in l_hash) {
        return { error : l_hash.error };
    } else {
        l_hash = l_hash.hash;
    }

    var joined_dataset = [],
        p_hash = join_type === "right" ? r_hash : l_hash,
        f_hash = join_type === "right" ? l_hash : r_hash,
        empty_outer_row = f_hash[Object.keys(f_hash)[0]][0].map(function () {
            return '';
        }),
        original_num_columns = Object.keys(columns).length;

    join_type = typeof(join_type) === "undefined" ? "inner" : join_type;

    Object.keys(p_hash).forEach(function (key_field) {
        if (key_field in f_hash) {
            p_hash[key_field].forEach(function (p_row) {
                f_hash[key_field].forEach(function (f_row) {
                    joined_dataset.push(
                        join_type === "right" ? f_row.concat(p_row)
                                              : p_row.concat(f_row)
                    );
                });
            });
        } else if (join_type !== "inner") {
            p_hash[key_field].forEach(function (p_row) {
                joined_dataset.push(
                    join_type === "right" ? empty_outer_row.concat(p_row)
                                          : p_row.concat(empty_outer_row)
                );
            });
        }
    });

    Object.keys(f_columns).forEach(function (column_name) {
        var column = f_columns[column_name];
        column["index"] += original_num_columns;

        columns[column_name] = column;
    });

    rows = _prepare_rows(joined_dataset);

    return {};
};

var _group = function (data) {
    var hashed_dataset = _hash_dataset_by_key_columns(data), group_by = data.key_columns;
    var grouped_dataset = [], errors = [];

    if ("error" in hashed_dataset) {
        return { "error" : hashed_dataset.error };
    } else {
        hashed_dataset = hashed_dataset.hash;
    }

    Object.keys(hashed_dataset).forEach(function (key) {
        var grouped_row = [];

        hashed_dataset[key].forEach(function (row) {
            Object.keys(columns).forEach(function (column_name) {
                var agg_type = columns[column_name]["agg_type"],
                    index = columns[column_name]["index"],
                    is_key_column = group_by.indexOf(column_name) !== -1;

                if (index in grouped_row && !is_key_column) {
                    if (agg_type === "sum") {
                        grouped_row[index] += row[index];
                    } else if (agg_type === "max") {
                        grouped_row[index] = ( grouped_row[index] < row[index] )
                                           ? row[index]
                                           : grouped_row[index];
                    } else if (agg_type === "min") {
                        grouped_row[index] = ( grouped_row[index] > row[index] )
                                           ? row[index]
                                           : grouped_row[index];
                    } else {
                        errors.push(
                            "Unrecognized agg_type for columm '" + column_name + "'."
                        );
                    }
                } else {
                    grouped_row[index] = row[index];
                }
            });
        });

        grouped_dataset.push(grouped_row);
    });

    rows = _prepare_rows(grouped_dataset);

    return errors.length > 0 ? { error : errors.join("\n") } : {};
};

var _partition = function (data) {
    var hashed_dataset = _hash_dataset_by_key_columns(data, true);

    if ("error" in hashed_dataset) {
        return { "error" : hashed_dataset.error };
    } else {
        hashed_dataset = hashed_dataset.hash;
    }

    partitioned_by = data.key_columns;

    var columns_row = Object.keys(columns).map(function (column_name) {
        return columns[column_name];
    }).sort(function (a, b) {
        return _num_sort(a["index"], b["index"]);
    });

    Object.keys(hashed_dataset).forEach(function (key) {
        var dataset = hashed_dataset[key];
        partitioned_rows[key] = dataset;
    });

    return {};
};

var _get_partition_keys = function (data) {
    var keys =  Object.keys(partitioned_rows).map(function (key) {
        return key.split("|");
    });

    return { keys : keys };
};

var _get_partitioned = function (data) {
    return { rows : _get_visible_rows(undefined, partitioned_rows[data.key]) };
};

var _get_dataset = function (data) {
    return { rows : _get_visible_rows(data.column_names) };
};

var _get_distinct_consecutive_rows = function (data) {
    var visible_rows = _get_visible_rows([ data.column_name ]),
        distinct_consecutive_rows = [],
        current_row = 0,
        current_value;

    visible_rows.forEach(function (row, i) {
        if (!i || (!row.parent_row && (current_value != row[0]))) {
            if (distinct_consecutive_rows.length) {
                distinct_consecutive_rows[current_row++][2] = i - 1;
            }

            current_value = row[0];
            distinct_consecutive_rows.push([ current_value, i, i ]);
        }
    });

    if (distinct_consecutive_rows.length) {
        distinct_consecutive_rows[current_row++][2] = visible_rows.length - 1;
    }

    return { distinct_rows : distinct_consecutive_rows };
};

var _get_number_of_records = function (data) {
    return { num_rows : _get_visible_rows().length };
};

var _paginate = function (data) {
    rows_per_page = data.rows_per_page;

    return {};
};

var _get_last_page = function () {
    var visible_rows = _get_visible_rows();

    return {
        visible_rows: visible_rows,
        last_page: (Math.ceil(visible_rows.length / rows_per_page) - 1)
    };
};

var _get_page = function (data) {
    var row_data = _get_last_page(),
        start, end;

    data.last_page = row_data.last_page;

    _set_page(data);

    start = rows_per_page * current_page;
    end = start + rows_per_page;

    return { rows : row_data.visible_rows.slice(start, end), current_page : current_page + 1 };
};

var _set_page = function (data) {

    if (!("last_page" in data)) {
        data.last_page = _get_last_page().last_page;
    }

    if (typeof(data.page_num) !== "undefined") {
        current_page = data.page_num - 1;
    } else if (typeof(current_page) === "undefined") {
        current_page = 0;
    } else if (data.increment_page) {
        current_page++;
    } else if (data.decrement_page) {
        current_page--;
    }

    if (current_page < 0) current_page = 0;
    if (current_page > data.last_page) current_page = data.last_page;

    return { current_page: current_page + 1 };
};

var _get_number_of_pages = function () {
    var row_data = _get_last_page();

    return { number_of_pages: row_data.last_page + 1 };
};

var _hide_columns = function (data) {
    if ("column_names" in data) {
        data.column_names.forEach(function (column) {
            if (column in columns) {
                columns[column]["is_visible"] = false;
            }
        });
    } else if ("column_name_regex" in data) {
        Object.keys(columns).forEach(function (column) {
            if (column.match(data.column_name_regex)) {
                columns[column]["is_visible"] = false;
            }
        });
    }

    return {};
};

var _show_columns = function (data) {
    if ("column_names" in data) {
        data.column_names.forEach(function (column) {
            if (column in columns) {
                columns[column]["is_visible"] = true;
            }
        });
    } else if ("column_name_regex" in data) {
        Object.keys(columns).forEach(function (column) {
            if (column.match(data.column_name_regex)) {
                columns[column]["is_visible"] = true;
            }
        });
    }

    return {};
};

var _hide_all_columns = function (data) {
    Object.keys(columns).forEach(function (column) {
        columns[column]["is_visible"] = false;
    });

    return {};
};

var _show_all_columns = function (data) {
    Object.keys(columns).forEach(function (column) {
        columns[column]["is_visible"] = true;
    });

    return {};
};

var _get_all_columns = function (data) {
    return { columns : columns };
};

var _get_columns = function (data) {
    return { columns : _get_visible_columns() };
};

var _get_rows = function (data) {
    var start = data.start, end = data.end;

    if (typeof(end) !== "undefined") {
        end += 1;
    }

    return { rows : _get_dataset(data).rows.slice(start, end) };
};

var _get_expected_num_rows = function (data) {
    return { ex_num_rows : expected_num_rows };
};

var _request_dataset = function (data) {
    var request_made = false;

    columns           = {};
    rows              = [];
    expected_num_rows = undefined;

    if (typeof(socket) !== "undefined") {
        socket.send(data.request);
        request_made = true;
    }
    if (typeof(ajax_datasource) !== "undefined") {
        _ajax(data.request);
        request_made = true;
    }

    return request_made ? {} : { error: "Could not request dataset; no datasource defined." };
};

var _request_dataset_for_append = function (data) {
    var request_made = false;

    if (typeof(socket) !== "undefined") {
        socket.send(data.request);
        request_made = true;
    }
    if (typeof(ajax_datasource) !== "undefined") {
        _ajax(data.request);
        request_made = true;
    }

    return request_made ? {} : { error: "Could not request dataset; no datasource defined." };
};

var _finish = function () {
    if (typeof(on_socket_close) !== "undefined" && socket.readyState !== 0 && socket.readyState !== 3) {
        socket.send(on_socket_close);
    }
};

var _add_child_rows = function (data) {
    var new_rows = _prepare_rows(data.rows),
        row_hash = _hash_rows_by_key_columns([ data.join_on ], new_rows, {}, true),
        join_idx = columns[data.join_on].index;

    rows.forEach(function (row, i) {
        var row_values = row.row,
            children   = row_hash[row_values[join_idx]];

        if (children) {
            has_child_elements = true;
            row.has_children = true;
            row.children = children;
            children.forEach(function (child) {
                child.parent_row = row;
                child.is_visible = row.is_visible;
            });
        }
    });

    return {};
};

handle_message = function (e) {
    var data = e.data, reply = {};

    if (typeof(data) === "undefined") {
        return;
    }

    if (data.cmd === "initialize") {
        var wait_to_connect = _initialize(data);

        if (wait_to_connect) {
            var wait = function () {
                if (is_ws_ready) {
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
            case "set_decimal_mark_character":
                reply = _set_decimal_mark_character(data);
                break;
            case "apply_filter":
                reply = _apply_filter(data);
                break;
            case "clear_filters":
                reply = _clear_filters(data);
                break;
            case "filter":
                reply = _filter(data);
                break;
            case "apply_limit":
                reply = _apply_limit(data);
                break;
            case "limit":
                reply = _limit(data);
                break;
            case "remove_columns":
                reply = _remove_columns(data);
                break;
            case "prepend_column_names":
                reply = _prepend_column_names(data);
                break;
            case "alter_column_name":
                reply = _alter_column_name(data);
                break;
            case "alter_column_sort_type":
                reply = _alter_column_sort_type(data);
                break;
            case "alter_column_agg_type":
                reply = _alter_column_agg_type(data);
                break;
            case "alter_column_title":
                reply = _alter_column_title(data);
                break;
            case "append":
                reply = _append(data);
                break;
            case "hash":
                reply = _hash_dataset_by_key_columns(data);
                break;
            case "join":
                reply = _join_hashes(data);
                break;
            case "group":
                reply = _group(data);
                break;
            case "partition":
                reply = _partition(data);
                break;
            case "get_partition_keys":
                reply = _get_partition_keys(data);
                break;
            case "get_partitioned":
                reply = _get_partitioned(data);
                break;
            case "paginate":
                reply = _paginate(data);
                break;
            case "get_page":
                reply = _get_page(data);
                break;
            case "set_page":
                reply = _set_page(data);
                break;
            case "hide_columns":
                reply = _hide_columns(data);
                break;
            case "show_columns":
                reply = _show_columns(data);
                break;
            case "hide_all_columns":
                reply = _hide_all_columns(data);
                break;
            case "show_all_columns":
                reply = _show_all_columns(data);
                break;
            case "get_all_columns":
                reply = _get_all_columns(data);
                break;
            case "get_columns":
                reply = _get_columns(data);
                break;
            case "get_rows":
                reply = _get_rows(data);
                break;
            case "get_dataset":
                reply = _get_dataset(data);
                break;
            case "get_distinct_consecutive_rows":
                reply = _get_distinct_consecutive_rows(data);
                break;
            case "get_num_rows":
                reply = _get_number_of_records(data);
                break;
            case "get_expected_num_rows":
                reply = _get_expected_num_rows(data);
                break;
            case "get_number_of_pages":
                reply = _get_number_of_pages(data);
                break;
            case "request_dataset":
                reply = _request_dataset(data);
                break;
            case "request_dataset_for_append":
                reply = _request_dataset_for_append(data);
                break;
            case "refresh":
                reply["columns"] = _get_visible_columns();
                reply["rows"]    = _get_visible_rows();
                break;
            case "refresh_all":
                reply["columns"] = columns;
                reply["rows"]    = rows.map(function (row) { return row.row; });
                break;
            case "finish":
                _finish();
                break;
            case "add_child_rows":
                reply = _add_child_rows(data);
                break;
            default:
                reply["error"] = "Unrecognized jdata_worker command: " + data.cmd;
        }
    }

    postMessage(reply);
};

}

})();
