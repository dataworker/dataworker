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
    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
};

self.addEventListener('message', function (e) {
    var data = e.data;
    if (typeof(data) === "undefined") {
        return;
    }

    var sort_on = data.sort_on;
    var columns = data.columns;
    var rows    = data.rows;

    rows.sort(function (a, b) {
        var i, sort_column, column_name, reverse, sort_type, sort_result, val_a, val_b;

        for (i = 0; i < sort_on.length; i++) {
            sort_column = sort_on[i].match(/(-?)(\w+)/);
            column_name = sort_column[2];
            reverse     = !!sort_column[1];
            sort_type   = columns[column_name]["sort_type"];

            if (reverse) {
                val_b = a[columns[column_name]["index"]];
                val_a = b[columns[column_name]["index"]];
            } else {
                val_a = a[columns[column_name]["index"]];
                val_b = b[columns[column_name]["index"]];
            }

            if (typeof(sort_type) === "function") {
                sort_result = sort_type(val_a, val_b);
            } else if (sort_type === "alpha") {
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

    self.postMessage({ rows: rows });
}, false);
