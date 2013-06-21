var columns, rows,
    rows_per_page = 10, current_page = 0;

var _prepare_columns = function (raw_columns) {
    var prepared_columns = {};

    raw_columns.forEach(function (column, i) {
        var name = typeof(column) === "string" ? column : column["name"];

        prepared_columns[name] = {
            sort_type : column["sort_type"] || "alpha",
            agg_type  : column["agg_type"]  || "max",
            title     : column["title"]     || name,
            index     : i
        };
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

var _initialize = function (data) {
    columns = _prepare_columns(data.columns);
    rows    = _prepare_rows(data.rows);

    return {
        columns : columns,
        rows    : _strip_row_metadata(rows)
    };
};

var _alpha_sort = function (a, b) {
    if (typeof(a) === "undefined" || typeof(b) === "undefined") return 0;

    a = a.toLowerCase();
    b = b.toLowerCase();

    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
};

var _num_sort = function (a, b) {
    a = parseFloat(a);
    b = parseFloat(b);

    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
};

var _sort = function (data) {
    var sort_on = data.sort_on;

    rows.sort(function (a, b) {
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
    });

    return {};
};

var _apply_filter = function (data) {
    var regex = data.regex, relevant_columns = data.relevant_columns;
    var i, column
        relevant_indexes = relevant_columns.map(function (column) {
            return columns[column]["index"];
        });

    rows.forEach(function (row) {
        row["is_visible"] = false;

        for (i = 0; i < row["row"].length; i++) {
            column = row["row"][i];

            if (
                (relevant_indexes.length === 0 || relevant_indexes.indexOf(i) !== -1)
                && column !== null && column.match(regex)
            ) {
                row["is_visible"] = true;
                break;
            }
        }
    });

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
    var new_columns = data.new_columns, new_rows = data.new_rows;
    var reply = {};

    var error = _check_columns_for_append(new_columns);
    if (!error) {
        rows = rows.concat(_prepare_rows(new_rows));
    } else {
        reply["error"] = error;
    }

    return reply;
};

var _hash_dataset_by_key_columns = function (data) {
    var key_columns = data.key_columns;
    var key_indexes, hash = {}, errors = [];

    key_columns = key_columns instanceof Array ? key_columns : [ key_columns ];
    key_columns.forEach(function (column) {
        if (!column in columns) {
            errors.push("Column '" + column + "' not in dataset.");
        }
    });

    key_indexes = key_columns.map(function (column_name) {
        return columns[column_name]["index"];
    });

    rows.map(function (row) { return row["row"]; }).forEach(function (row) {
        var key = key_indexes.map(function (i) { return row[i]; }).join("|");

        if (key in hash) {
            hash[key].push(row);
        } else {
            hash[key] = [ row ];
        }
    });

    return {
        hash  : hash,
        error : errors.join("\n")
    };
};

var _join_hashes = function (data) {
    var l_hash = data.l_hash, r_hash = data.r_hash,
        join_type = data.join_type, f_columns = data.f_columns;
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
    var hashed_dataset = data.hashed_dataset, group_by = data.group_by;
    var grouped_dataset = [], errors = [];

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
    var hashed_dataset = data.hashed_dataset;
    var columns_row = Object.keys(columns).map(function (column_name) {
            var column = columns[column_name];

            return column;
        }).sort(function (a, b) {
            return _num_sort(a["index"], b["index"]);
        }), partitioned_datasets = {};

    Object.keys(hashed_dataset).forEach(function (key) {
        var dataset = hashed_dataset[key];
        partitioned_datasets[key] = dataset;
    });

    return { partitioned : partitioned_datasets };
};

var _get_dataset = function (data) {
    var column_names = data.column_names;
    var visible_rows = rows.filter(function (row) { return row["is_visible"]; })
                           .map(function (row) { return row["row"]; }),
        filtered_dataset = [];

    if (column_names.length === 0) {
        return { rows : visible_rows };
    }

    column_idxs = column_names.map(function (column) {
        return columns[column]["index"];
    });

    visible_rows.forEach(function (row, i) {
        var filtered_row = [];

        column_idxs.forEach(function (column_idx, i) {
            filtered_row[i] = row[column_idx];
        });

        filtered_dataset[i] = filtered_row;
    });

    return { rows : filtered_dataset };
};

var _get_number_of_records = function (data) {
    return { num_rows : rows.length };
};

var _paginate = function (data) {
    rows_per_page = data.rows_per_page;
    current_page = 0;

    return {};
};

var _get_page = function (data) {
    var page_num = data.page_num,
        post_increment_page = data.post_increment_page,
        pre_decrement_page = data.pre_decrement_page;
    var start, end;

    if (pre_decrement_page) page_num = current_page;

    current_page = typeof(page_num) !== "undefined" ? (page_num - 1)
                                                    : current_page;
    if (current_page < 0) current_page = 0;

    start = rows_per_page * current_page;
    end = start + rows_per_page;

    if (post_increment_page) current_page++;

    return { rows : _strip_row_metadata(rows.slice(start, end)) };
};

var _set_page = function (data) {
    var page_num = data.page_num;

    current_page = page_num > 0 ? (page_num - 1) : 0;

    return {};
};

var _get_columns = function (data) {
    return { columns : columns };
};

var _get_rows = function (data) {
    var start = data.start,
        end = data.end;

    if (typeof(end) !== "undefined") {
        end += 1;
    }

    return { rows : _strip_row_metadata(rows.slice(start, end)) };
};

self.addEventListener("message", function (e) {
    var data = e.data, reply = {};

    if (typeof(data) === "undefined") {
        return;
    }

    switch (data.cmd) {
        case "initialize":
            reply = _initialize(data);
            break;
        case "sort":
            reply = _sort(data);
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
        case "paginate":
            reply = _paginate(data);
            break;
        case "get_page":
            reply = _get_page(data);
            break;
        case "set_page":
            reply = _set_page(data);
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
        case "get_num_rows":
            reply = _get_number_of_records(data);
            break;
        case "refresh":
            reply["columns"] = columns;
            reply["rows"]    = _strip_row_metadata(rows);
            break;
    }

    self.postMessage(reply);
}, false);
