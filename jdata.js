var JData;

(function() {
    "use strict";

    JData = function (dataset) {
        var self = this instanceof JData ? this : Object.create(JData.prototype);
        var columns = dataset.slice(0, 1)[0];

        self._columns = columns.map(function (column) {
            var name = typeof(column) === "string" ? column : column['name'];
            return name;
        });
        self._columns_idx_xref = self._build_columns_idx_xref();
        self._columns_metadata = self._build_columns_metadata(columns);

        self._dataset = dataset.slice(1);

        self._rows_per_page = 10;
        self._current_page  = 0;

        return self;
    };

    JData.prototype._build_columns_idx_xref = function () {
        var self = this;
        var columns_idx_xref = {};

        self._columns.forEach(function (column, i) {
            columns_idx_xref[column] = i;
        });

        return columns_idx_xref;
    };

    JData.prototype._build_columns_metadata = function (columns) {
        var self = this;
        var columns_metadata = {};

        columns.forEach(function (column) {
            var column_name = typeof(column) === "string" ? column : column['name'];

            columns_metadata[column_name] = {
                sort_type: column['sort_type'] || 'alpha',
                agg_type: column['agg_type'] || 'max'
            };
        });

        return columns_metadata;
    };

    JData.prototype.get_columns = function () {
        return this._columns;
    };

    JData.prototype.get_dataset = function () {
        var self = this, columns = Array.prototype.slice.call(arguments);
        var filtered_dataset = [];

        if (columns.length === 0) return self._dataset;

        columns = columns.map(function (column) {
            return self._columns_idx_xref[column];
        });

        self._dataset.forEach(function (row, i) {
            var filtered_row = [];

            columns.forEach(function (column_idx, i) {
                filtered_row[i] = row[column_idx];
            });

            filtered_dataset[i] = filtered_row;
        });

        return filtered_dataset;
    };

    JData.prototype.filter = function () {
        var self = this, regex = arguments[0],
                         relevant_columns = Array.prototype.slice.call(arguments, 1);
        var i = 0, j, column, filtered_dataset = [],
            relevant_indexes = relevant_columns.map(function (column) {
                return self._columns_idx_xref[column];
            });

        self._dataset.forEach(function (row) {
            for (j = 0; j < row.length; j++) {
                column = row[j];

                if (
                    (relevant_indexes.length == 0 || relevant_indexes.indexOf(j) != -1)
                    && column !== null
                    && column.match(regex)
                ) {
                    filtered_dataset[i++] = row;
                    break;
                }
            }
        });

        self._dataset = filtered_dataset;

        return self;
    };

    JData.prototype.limit = function (num_rows) {
        var self = this;

        self._dataset = self._dataset.slice(0, num_rows);

        return self;
    };

    JData.prototype.sort = function () {
        var self = this, columns = Array.prototype.slice.call(arguments);

        self._dataset.sort(function (a, b) {
            var i, sort_column, column_name, reverse, sort_type, sort_result, val_a, val_b;

            for (i = 0; i < columns.length; i++) {
                sort_column = columns[i].match(/(-?)(\w+)/);
                column_name = sort_column[2];
                reverse     = !!sort_column[1];
                sort_type   = self._columns_metadata[column_name]['sort_type'];

                if (reverse) {
                    val_b = a[self._columns_idx_xref[column_name]];
                    val_a = b[self._columns_idx_xref[column_name]];
                } else {
                    val_a = a[self._columns_idx_xref[column_name]];
                    val_b = b[self._columns_idx_xref[column_name]];
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

    JData.prototype.remove_columns = function () {
        var self = this, columns_to_remove = Array.prototype.slice.call(arguments);
        var i = 0, filtered_columns_idx_xref = {}, filtered_dataset = [];

        self._columns.forEach(function (column) {
            if (columns_to_remove.indexOf(column) === -1) {
                filtered_columns_idx_xref[column] = self._columns_idx_xref[column];
            }
        });
        self._columns = Object.keys(filtered_columns_idx_xref);
        filtered_columns_idx_xref = self._build_columns_idx_xref();

        self._dataset.forEach(function (row) {
            var filtered_row = [];

            self._columns.forEach(function (column) {
                filtered_row[filtered_columns_idx_xref[column]] = row[self._columns_idx_xref[column]];
            });

            filtered_dataset[i++] = filtered_row;
        });

        self._dataset = filtered_dataset;

        return self;
    };

    JData.prototype.paginate = function (rows_per_page) {
        var self = this;

        self._rows_per_page = rows_per_page;
        self._current_page = 0;

        return self;
    };

    JData.prototype.get_next_page = function () {
        var self = this, page;

        page = self.get_page();
        self._current_page++;

        return page;
    };

    JData.prototype.get_previous_page = function () {
        var self = this;

        self._current_page--;

        return self.get_page();
    };

    JData.prototype.get_page = function (page_num) {
        var self = this;
        var start, end;

        self._current_page = typeof(page_num) != "undefined" ? (page_num - 1)
                                                             : self._current_page;
        if (self._current_page < 0) self._current_page = 0;

        start = self._rows_per_page * self._current_page;
        end   = start + self._rows_per_page;

        return self._dataset.slice(start, end);
    };

    JData.prototype.set_page = function (page_num) {
        var self = this;

        self._current_page = page_num > 0 ? (page_num - 1) : 0;

        return self;
    };

    JData.prototype.append = function (data) {
        var self = this;
        var columns, rows;

        if (data instanceof JData) {
            columns = data._columns;
            rows = data._dataset;
        } else {
            columns = data.slice(0, 1)[0];
            rows = data.slice(1);
        }

        self._check_columns_for_append(columns);
        self._dataset = self._dataset.concat(rows);

        return self;
    };

    JData.prototype._check_columns_for_append = function (columns) {
        var self = this;
        var i, error = "Cannot append dataset (columns do not match):\n\t"
                     + self._columns + "\n\t\tVS\n\t" + columns;

        if (columns.length == self._columns.length) {
            for (i = 0; i < columns.length; i++) {
                if (columns[i] !== self._columns[i]) {
                    throw new Error(error);
                }
            }
        } else {
            throw new Error(error);
        }
    };

    JData.prototype.join = function (fdata, pk, fk, join_type) {
        var self = this;
        var joined_dataset = [],
            p_hash = {}, p_idx = self._columns_idx_xref[pk],
            f_hash = {}, f_idx = fdata._columns_idx_xref[fk];

        fdata._columns.forEach(function (column) {
            if (self._columns.indexOf(column) != -1) {
                throw new Error(
                    "Joining dataset has a column with the same name as an existing column: "
                    + column
                );
            }
        });

        self._dataset.forEach(function (row) {
            var field = row[p_idx];

            if (field in p_hash) {
                p_hash[field].push(row);
            } else {
                p_hash[field] = [ row ];
            }
        });
        fdata._dataset.forEach(function (row) {
            var field = row[f_idx];

            if (field in f_hash) {
                f_hash[field].push(row);
            } else {
                f_hash[field] = [ row ];
            }
        });

        if (typeof(join_type) === "undefined") {
            Object.keys(p_hash).forEach(function (key_field) {
                if (key_field in f_hash) {
                    p_hash[key_field].forEach(function (p_row) {
                        f_hash[key_field].forEach(function (f_row) {
                            joined_dataset.push(p_row.concat(f_row));
                        });
                    });
                }
            });
        } else if (join_type === "left") {
            Object.keys(p_hash).forEach(function (key_field) {
                if (key_field in f_hash) {
                    p_hash[key_field].forEach(function (p_row) {
                        f_hash[key_field].forEach(function (f_row) {
                            joined_dataset.push(p_row.concat(f_row));
                        });
                    });
                } else {
                    p_hash[key_field].forEach(function (p_row) {
                        joined_dataset.push(
                            p_row.concat(fdata._columns.map(function () { return '' }))
                        );
                    });
                }
            });
        } else if (join_type === "right") {
            Object.keys(f_hash).forEach(function (key_field) {
                if (key_field in p_hash) {
                    p_hash[key_field].forEach(function (p_row) {
                        f_hash[key_field].forEach(function (f_row) {
                            joined_dataset.push(p_row.concat(f_row));
                        });
                    });
                } else {
                    f_hash[key_field].forEach(function (f_row) {
                        joined_dataset.push(
                            self._columns.map(function() { return '' }).concat(f_row)
                        );
                    });
                }
            });
        } else {
            throw new Error("Unknown join type.");
        }

        self._columns = self._columns.concat(fdata._columns);
        self._columns_idx_xref = self._build_columns_idx_xref();
        Object.keys(fdata._columns_metadata).forEach(function (column) {
            self._columns_metadata[column] = fdata._columns_metadata[column];
        });

        self._dataset = joined_dataset;

        return self;
    };

    JData.prototype.prepend_column_names = function (prepend) {
        var self = this;
        var columns_metadata = {};

        self._columns = self._columns.map(function (column) { return prepend + column; });
        self._columns_idx_xref = self._build_columns_idx_xref();
        Object.keys(self._columns_metadata).forEach(function (column) {
            columns_metadata[prepend + column] = self._columns_metadata[column];
        });

        self._columns_metadata = columns_metadata;

        return self;
    };

    JData.prototype.group = function () {
        var self = this, columns_to_group_by = Array.prototype.slice.call(arguments);

        return self;
    };
})();
