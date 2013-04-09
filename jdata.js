var JData;

(function() {
    "use strict";

    JData = function (dataset) {
        var self = this instanceof JData ? this : Object.create(JData.prototype);

        self._columns = self._prepare_columns(dataset.slice(0, 1)[0]);
        self._dataset = dataset.slice(1);

        self._rows_per_page = 10;
        self._current_page  = 0;

        self._partitioned_datasets = {};

        self._render_function = function () {};

        self._action_queue = [];
        self._is_in_action = false;

        return self;
    };

    JData.prototype._queue_next = function (action) {
        var self = this;

        self._action_queue.push(action);
        self._next_action();

        return self;
    };

    JData.prototype._next_action = function (finish_previous) {
        var self = this;

        if (finish_previous) self._is_in_action = false;

        if (!self._is_in_action && self._action_queue.length > 0) {
            var action = self._action_queue.shift();
            self._is_in_action = true;
            action();
        }

        return self;
    };

    JData.prototype._prepare_columns = function (columns) {
        var self = this, prepared_columns = {};

        columns.forEach(function (column, i) {
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

    JData.prototype.get_columns = function (callback) {
        var self = this;

        self._queue_next(function () {
            callback(self._columns);

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype._get_dataset = function () {
        var self = this,
            callback = arguments[0],
            column_names = arguments[1] instanceof Array
                         ? arguments[1]
                         : Array.prototype.slice.call(arguments, 1);
        var filtered_dataset = [], column_indexes;

        if (column_names.length === 0) {
            callback(self._dataset);
        } else {
            column_indexes = column_names.map(function (name) {
                return self._columns[name]["index"];
            });

            self._dataset.forEach(function (row, i) {
                var filtered_row = [];

                column_indexes.forEach(function (column_index) {
                    filtered_row.push(row[column_index]);
                });

                filtered_dataset.push(filtered_row);
            });

            callback(filtered_dataset);
        }

        return self._next_action(true);
    };

    JData.prototype.get_dataset = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._get_dataset.apply(self, args);
        });

        return self;
    };

    JData.prototype._filter = function () {
        var self = this,
            regex = arguments[0],
            relevant_columns = arguments[1] instanceof Array
                             ? arguments[1]
                             : Array.prototype.slice.call(arguments, 1);
        var i = 0, j = 0, k = 0, filtered_dataset = [],
            relevant_indexes = relevant_columns.map(function (column) {
                return self._columns[column]["index"];
            });

        var next = function () {
            var row = self._dataset[i++], column;

            for (j = 0; j < row.length; j++) {
                column = row[j];

                if (
                    (relevant_indexes.length == 0 || relevant_indexes.indexOf(j) != -1)
                    && column !== null
                    && column.match(regex)
                ) {
                    filtered_dataset[k++] = row;
                    break;
                }
            };

            if (i < self._dataset.length) {
                setTimeout(next, 0);
            } else {
                self._dataset = filtered_dataset;

                return self._next_action(true);
            }
        };

        setTimeout(next, 0);

        return self;
    };

    JData.prototype.filter = function () {
        var self = this, args = Array.prototype.slice.call(arguments);
        
        self._queue_next(function () {
            self._filter.apply(self, args);
        });

        return self;
    };

    JData.prototype._limit = function (num_rows) {
        var self = this;

        self._dataset = self._dataset.slice(0, num_rows);

        return self._next_action(true);
    };

    JData.prototype.limit = function (num_rows) {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._limit.apply(self, args);
        });

        return self;
    };

    JData.prototype._merge_sort = function (sort_function) {
        var self = this;
        var temp = [], sort_queue = [];

        var next_sort_action = function () {
            if (sort_queue.length > 0) {
                var action = sort_queue.shift();
                action();
            } else {
                self._next_action(true);
            }
        };

        var m_sort = function (array, temp, left, right) {
            var mid;

            if (right > left) {
                mid = Math.floor((right + left) / 2);

                m_sort(array, temp, left, mid);
                m_sort(array, temp, mid + 1, right);

                sort_queue.push(function () {
                    merge(array, temp, left, mid + 1, right);
                });
            }
        };

        var merge = function (array, temp, left, mid, right) {
            var left_end, num_elements, tmp_pos;

            left_end = mid - 1;
            tmp_pos = left;
            num_elements = right - left + 1;

            var next_merge_step = function () {
                var i = 0;

                if (sort_function(array[left], array[mid]) <= 0) {
                    temp[tmp_pos++] = array[left++];
                } else {
                    temp[tmp_pos++]  = array[mid++];
                }

                if ( (left <= left_end) && (mid <= right) ) {
                    setTimeout(next_merge_step, 0);
                } else {
                    while (left <= left_end) {
                        temp[tmp_pos++] = array[left++];
                    }
                    while (mid <= right) {
                        temp[tmp_pos++] = array[mid++];
                    }

                    var next_copy_back_step = function () {
                        array[right] = temp[right];
                        right = right - 1;

                        if (i++ < num_elements) {
                            setTimeout(next_copy_back_step, 0);
                        } else {
                            return next_sort_action();
                        }
                    };

                    setTimeout(next_copy_back_step, 0);
                }
            };

            setTimeout(next_merge_step, 0);
        };

        m_sort(self._dataset, temp, 0, self._dataset.length - 1);
        next_sort_action();

        return self;
    };

    JData.prototype._sort = function () {
        var self = this,
            columns = arguments[0] instanceof Array
                    ? arguments[0]
                    : Array.prototype.slice.call(arguments);

        self._merge_sort(function (a, b) {
            var i, sort_column, column_name, reverse, sort_type, sort_result, val_a, val_b;

            for (i = 0; i < columns.length; i++) {
                sort_column = columns[i].match(/(-?)(\w+)/);
                column_name = sort_column[2];
                reverse     = !!sort_column[1];
                sort_type   = self._columns[column_name]["sort_type"];

                if (reverse) {
                    val_b = a[self._columns[column_name]["index"]];
                    val_a = b[self._columns[column_name]["index"]];
                } else {
                    val_a = a[self._columns[column_name]["index"]];
                    val_b = b[self._columns[column_name]["index"]];
                }

                if (typeof(sort_type) === "function") {
                    sort_result = sort_type(val_a, val_b);
                } else if (sort_type === "alpha") {
                    sort_result = self._alpha_sort(val_a, val_b);
                } else if (sort_type === "num") {
                    sort_result = self._num_sort(val_a, val_b);
                } else {
                    throw new Error("Unknown sort type.");
                }

                if (sort_result !== 0) {
                    return sort_result;
                }
            }

            return 0;
        });

        return self;
    };

    JData.prototype.sort = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._sort.apply(self, args);
        });

        return self;
    };

    JData.prototype._alpha_sort = function (a, b) {
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

    JData.prototype._num_sort = function (a, b) {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return 1;
        } else {
            return 0;
        }
    };

    JData.prototype._remove_columns = function () {
        var self = this,
            columns_to_remove = arguments[0] instanceof Array
                              ? arguments[0]
                              : Array.prototype.slice.call(arguments);
        var i = 0, columns_to_keep = {}, filtered_dataset = [];

        Object.keys(self._columns).forEach(function (column_name, i) {
            if (columns_to_remove.indexOf(column_name) === -1) {
                var column = self._columns[column_name];
                column["index"] = i;

                columns_to_keep[column_name] = column;
            }
        });

        var next = function () {
            var row = self._dataset[i++], filtered_row = [];

            Object.keys(columns_to_keep).forEach(function (column_name) {
                filtered_row[columns_to_keep[column_name]["index"]] = row[self._columns[column_name]["index"]];
            });

            filtered_dataset.push(filtered_row);

            if (i < self._dataset.length) {
                setTimeout(next, 0);
            } else {
                self._dataset = filtered_dataset;
                self._columns = columns_to_keep;

                return self._next_action(true);
            }
        };

        setTimeout(next, 0);

        return self;
    };

    JData.prototype.remove_columns = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._remove_columns.apply(self, args);
        });

        return self;
    };

    JData.prototype.paginate = function (rows_per_page) {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._rows_per_page = rows_per_page;
            self._current_page = 0;

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.get_next_page = function (callback) {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self.get_page(callback, undefined, true);

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.get_previous_page = function (callback) {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self.get_page(callback, self._current_page);

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype._get_page = function (callback, page_num, post_increment_page) {
        var self = this;
        var start, end;

        self._current_page = typeof(page_num) != "undefined" ? (page_num - 1)
                                                             : self._current_page;
        if (self._current_page < 0) self._current_page = 0;

        start = self._rows_per_page * self._current_page;
        end   = start + self._rows_per_page;

        callback(self._dataset.slice(start, end));
        if (post_increment_page) self._current_page++;

        return self._next_action(true);
    };

    JData.prototype.get_page = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._get_page.apply(self, args);
        });

        return self;
    };

    JData.prototype.set_page = function (page_num) {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._current_page = page_num > 0 ? (page_num - 1) : 0;

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.get_columns_and_records = function (callback) {
        var self = this;

        self._queue_next(function () {
            callback(self._columns, self._dataset);
        });

        return self;
    };

    JData.prototype._append = function (data) {
        var self = this;
        var columns, rows;

        if (data instanceof JData) {
            data.get_columns_and_records(function (columns, rows) {
                self._check_columns_for_append(columns);
                self._dataset = self._dataset.concat(rows);

                self._next_action(true);
            })
        } else {
            columns = data.slice(0, 1)[0];
            rows = data.slice(1);

            self._check_columns_for_append(columns);
            self._dataset = self._dataset.concat(rows);

            return self._next_action(true);
        }
    };

    JData.prototype._extract_column_names_in_order = function (columns) {
        return Object.keys(columns).sort(function (a, b) {
            return self._num_sort(
                columns[a]["index"],
                columns[b]["index"]
            );
        }).map(function (column) {
            return columns[column]["name"];
        });
    };

    JData.prototype._check_columns_for_append = function (columns) {
        var self = this;
        var i, throw_error;

        if (columns instanceof Array) {
            columns = self._prepare_columns(columns);
        }

        throw_error = function () {
            var original_columns = self._extract_column_names_in_order(self._columns)
                                       .join(", ");
            var append_columns = self._extract_column_names_in_order(columns)
                                     .join(", ");

            throw new Error(
                "Cannot append dataset (columns do not match):\n\t"
                + original_columns + "\n\t\tVS\n\t" + append_columns
            );
        };

        if (Object.keys(columns).length === Object.keys(self._columns).length) {
            Object.keys(columns).forEach(function (name) {
                if (columns[name]["index"] !== self._columns[name]["index"]) {
                    throw_error();
                }
            });
        } else {
            throw_error();
        }
    };

    JData.prototype.append = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._append.apply(self, args);
        });

        return self;
    };

    JData.prototype.join = function (fdata, pk, fk, join_type) {
        var self = this, args = Array.prototype.slice.call(arguments);
        var p_hash, f_hash, finish;

        if (!pk.length || !fk.length) {
            throw new Error("No join key(s) provided.");
        }
        if (pk.length != fk.length) {
            throw new Error("Odd number of join keys.");
        }
        if (
            typeof(join_type) !== "undefined"
            && !(join_type === "left" || join_type === "right")
        ) {
            throw new Error("Unknown join type.");
        }

        self._queue_next(function () {
            fdata.get_columns(function (f_columns) {
                Object.keys(f_columns).forEach(function (column_name) {
                    if (column_name in self._columns) {
                        throw new Error("Column names overlap.");
                    }
                });

                finish = function (joined_dataset) {
                    var original_num_columns = Object.keys(self._columns).length;

                    Object.keys(f_columns).forEach(function (column_name) {
                        var column = f_columns[column_name];
                        column["index"] += original_num_columns;

                        self._columns[column_name] = column;
                    });

                    self._dataset = joined_dataset;
                };

                self._next_action(true);
            });
        });

        self._queue_next(function () {
            self._hash_dataset_by_key_columns.call(self, function (hash) {
                p_hash = hash;
            }, pk);
        });
        self._queue_next(function () {
            fdata.get_hash_of_dataset_by_key_columns.call(fdata, function (hash) {
                f_hash = hash;
                self._next_action(true);
            }, fk);
        });

        self._queue_next(function () {
            self._join_hashes.call(self, p_hash, f_hash, join_type, finish);
        });

        return self;
    };

    JData.prototype._hash_dataset_by_key_columns = function (callback, key_columns) {
        var self = this;
        var i = 0, key_indexes, hash = {};

        key_columns = key_columns instanceof Array ? key_columns : [ key_columns ];
        key_columns.forEach(function (column) {
            if (!column in self._columns) {
                throw new Error("Column '" + column + "' not in dataset.");
            }
        });

        key_indexes = key_columns.map(function (column_name) {
            return self._columns[column_name]["index"];
        });

        var next = function () {
            var row = self._dataset[i++],
                key = key_indexes.map(function (i) { return row[i]; }).join("|");

            if (key in hash) {
                hash[key].push(row);
            } else {
                hash[key] = [ row ];
            }

            if (i < self._dataset.length) {
                setTimeout(next, 0);
            } else {
                callback(hash);
                self._next_action(true);
            }
        };

        setTimeout(next, 0);

        return self;
    };

    JData.prototype.get_hash_of_dataset_by_key_columns = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._hash_dataset_by_key_columns.apply(self, args);
        });

        return self;
    };

    JData.prototype._join_hashes = function (l_hash, r_hash, join_type, finish) {
        var self = this;
        var i = 0, joined_dataset = [],
            p_hash = join_type === "right" ? r_hash : l_hash,
            f_hash = join_type === "right" ? l_hash : r_hash,
            key_fields = Object.keys(p_hash),
            empty_outer_row =  f_hash[Object.keys(f_hash)[0]][0].map( function () {
                return '';
            });

        join_type = typeof(join_type) === "undefined" ? "inner" : join_type;

        var next = function () {
            var key_field = key_fields[i++];

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

            if (i < key_fields.length) {
                setTimeout(next, 0);
            } else {
                finish(joined_dataset);
                return self._next_action(true);
            }
        };

        setTimeout(next, 0);

        return self;
    };

    JData.prototype.prepend_column_names = function (prepend) {
        var self = this;

        self._queue_next(function () {
            var new_columns = {};

            Object.keys(self._columns).forEach(function (column_name) {
                new_columns[prepend + column_name] = self._columns[column_name];
            });
            self._columns = new_columns;

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.alter_column_name = function (old_name, new_name) {
        var self = this;

        self._queue_next(function () {
            Object.keys(self._columns).forEach(function (column_name) {
                if (column_name !== old_name && column_name === new_name) {
                    throw new Error("Column " + new_name + " already exists in dataset.");
                }
            });

            self._columns[new_name] = self._columns[old_name];
            delete self._columns[old_name];

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.alter_column_sort_type = function (column, sort_type) {
        var self = this;

        self._queue_next(function () {
            self._columns[column]["sort_type"] = sort_type;

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.alter_column_aggregate_type = function (column, agg_type) {
        var self = this;

        self._queue_next(function () {
            self._columns[column]["agg_type"] = agg_type;

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype.alter_column_title = function (column, title) {
        var self = this;

        self._queue_next(function () {
            self._columns[column]["title"] = title;

            return self._next_action(true);
        });

        return self;
    };

    JData.prototype._group_hashed_dataset = function (hashed_dataset, group_by) {
        var self = this;
        var i = 0, key_columns = Object.keys(hashed_dataset), grouped_dataset = [];

        var next = function () {
            var key = key_columns[i++], grouped_row = [];

            hashed_dataset[key].forEach(function (row) {
                Object.keys(self._columns).forEach(function (column_name) {
                    var agg_type = self._columns[column_name]["agg_type"],
                        index = self._columns[column_name]["index"],
                        is_key_column = group_by.indexOf(column_name) !== -1;

                    if (index in grouped_row && !is_key_column) {
                        if (agg_type === "sum") {
                            grouped_row[index] += row[index];
                        } else if (agg_type === "max") {
                            grouped_row[index] = (grouped_row[index] < row[index])
                                               ? row[index]
                                               : grouped_row[index];
                        } else if (agg_type === "min") {
                            grouped_row[index] = (grouped_row[index] > row[index])
                                               ? row[index]
                                               : grouped_row[index];
                        } else {
                            throw new Error(
                                "Unrecognized agg_type for column '" + column_name + "'."
                            );
                        }
                    } else {
                        grouped_row[index] = row[index];
                    }
                });
            });

            grouped_dataset.push(grouped_row);

            if (i < key_columns.length) {
                setTimeout(next, 0);
            } else {
                self._dataset = grouped_dataset;
                return self._next_action(true);
            }
        };

        setTimeout(next, 0);

        return self;
    };

    JData.prototype.group = function () {
        var self = this,
            group_by = arguments[0] instanceof Array
                     ? arguments[0]
                     : Array.prototype.slice.call(arguments);
        var hashed_dataset;
        
        self._queue_next(function () {
            self._hash_dataset_by_key_columns.call(self, function (hash) {
                hashed_dataset = hash;
            }, group_by);
        });

        self._queue_next(function () {
            self._group_hashed_dataset.call(self, hashed_dataset, group_by);
        });

        return self;
    };

    JData.prototype._partition_hashed_dataset = function (hashed_dataset) {
        var self = this;
        var i = 0, key_columns = Object.keys(hashed_dataset), 
            columns_row = Object.keys(self._columns).map(function (column_name) {
                var column = self._columns[column_name];
                column["name"] = column_name;

                return column;
            }).sort(function (a, b) {
                return self._num_sort(a["index"], b["index"]);
            });

        var next = function () {
            var key = key_columns[i++], dataset = hashed_dataset[key];

            dataset.unshift(columns_row);
            self._partitioned_datasets[key] = new JData(dataset);

            if (i < key_columns.length) {
                setTimeout(next, 0);
            } else {
                self._next_action(true);
            }
        };

        setTimeout(next, 0);

        return self;
    };

    JData.prototype.partition = function () {
        var self = this,
            partition_by = arguments[0] instanceof Array
                         ? arguments[0]
                         : Array.prototype.slice.call(arguments);
        var hashed_dataset;

        self._queue_next(function () {
            self._hash_dataset_by_key_columns(function (hash) {
                hashed_dataset = hash;
            }, partition_by);
        });

        self._queue_next(function () {
            self._partition_hashed_dataset.call(self, hashed_dataset);
        });

        return self;
    };

    JData.prototype.get_partition_keys = function (callback) {
        var self = this;

        self._queue_next(function () {
            callback(Object.keys(self._partitioned_datasets).map(function (key) {
                return key.split("|");
            }));

            self._next_action(true);
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
            callback(self._partitioned_datasets[keys.join("|")]);

            self._next_action(true);
        });

        return self;
    };

    JData.prototype._render = function (render_function) {
        var self = this;

        if (typeof(render_function) === "function") {
            self._render_function = render_function;
        } else {
            self._render_function();
        }

        return self._next_action(true);
    };

    JData.prototype.render = function () {
        var self = this, args = Array.prototype.slice.call(arguments);

        self._queue_next(function () {
            self._render.apply(self, args);
        });

        return self;
    };

    JData.prototype.get_number_of_records = function (callback) {
        var self = this;

        self._queue_next(function () {
            callback(self._dataset.length);
            return self._next_action(true);
        });

        return self;
    };
})();
