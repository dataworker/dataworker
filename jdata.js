var JData;

(function() {
    "use strict";

    JData = function (dataset) {
        var self = this instanceof JData ? this : Object.create(JData.prototype);

        self._columns = dataset.slice(0, 1)[0];
        self._dataset = dataset.slice(1);

        self._columns_idx_xref = self._build_columns_idx_xref();

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

    JData.prototype.sort = function (columns) {
        var self = this;

        self._dataset.sort(function (a, b) {
            var i, column_name, sort_type, sort_result, val_a, val_b;

            for (i = 0; i < columns.length; i++) {
                column_name = columns[i]["column"];
                sort_type   = columns[i]["sort_type"];

                val_a = a[self._columns_idx_xref[column_name]];
                val_b = b[self._columns_idx_xref[column_name]];

                if (typeof(sort_type) === "function") {
                    sort_result = sort_type(val_a, val_b);
                } else if (sort_type === "alpha") {
                    sort_result = self._alpha_sort(val_a, val_b);
                } else {
                    sort_result = self._num_sort(val_a, val_b);
                }

                if (sort_result !== 0) {
                    return sort_result;
                }
            };

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
})();
