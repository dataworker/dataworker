var srcPath = (function () {
    var scripts = document.getElementsByTagName("script"),
        srcFile = scripts[scripts.length - 1].src;

    return srcFile.replace(/(.*[\/\\]).*$/, "$1");
    return srcFile.substr(0, srcFile.lastIndexOf("/") + 1);
})();

function getRandomWord() {
    var length = parseInt(Math.random() * 30),
        word = "", i;

    for (i = 0; i < length; i++) {
        word += String.fromCharCode(65 + parseInt(Math.random() * 61));
    }

    return word;
}

function generateRandomDataset(columnTypes, numRows) {
    var rows = [], i;

    for (i = 0; i < numRows; i++) {
        rows.push(columnTypes.reduce(function (row, type) {
            row.push(type === "num" ? Math.random() * 1000 : getRandomWord());
            return row;
        }, []));
    }

    return rows;
}

function outputRowsForStreaming(dataset, rowsPerLine) {
    var dataset = generateRandomDataset([ "num", "alpha" ], 300),
        output  = [];

    while (dataset.length) {
        output.push(JSON.stringify({ rows: dataset.splice(0, rowsPerLine) }));
    }

    console.log(output.join("\n"));
}
