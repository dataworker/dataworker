var JData;

(function() {
    "use strict";

    JData = function (dataset) {
        var self = this instanceof JData ? this : Object.create(JData.prototype);

        self._columns = dataset.slice(0, 1)[0];
        self._dataset = dataset.slice(1);

        self._columns_idx_xref = {};
        self._columns.forEach(function (column, i) {
            self._columns_idx_xref[column] = i;
        });

        return self;
    };

    JData.prototype.get_dataset = function (column_or_columns) {
        var self = this, columns, filtered_dataset = [];

        if (typeof(column_or_columns) === "undefined") {
            return self._dataset;
        } else if (column_or_columns instanceof Array) {
            columns = column_or_columns;
        } else {
            columns = [ column_or_columns ];
        }

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
    }

    JData.prototype.get_columns = function () {
        return this._columns;
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
})();
