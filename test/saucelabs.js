(function () {
    var log = [];

    QUnit.done(function (testResults) {
        testResults.tests = log.map(function (details) {
            return {
                name: details.name,
                result: details.result,
                expected: details.expected,
                actual: details.actual,
                source: details.source
            };
        });

        window.global_test_results = testResults;
    });

    QUnit.testStart(function (testDetails) {
        QUnit.log(function (details) {
            if (!details.result) {
                details.name = testDetails.name;
                log.push(details);
            }
        });
    });
})();
