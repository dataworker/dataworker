## v3.0.0

* Various documentation fixes
* Various test suite fixes - tests now work in Chrome, Firefox, and IE
* Fixed a bug with large datasets
* Fixed a bug where the single-threaded version did not send the final reply
* Test suite updated to use QUnit's new v2.x API
* Multiple webworker sources can now be provided and attempted - Blob is preferred by default
* New syntax available for instantiating DataWorker with a local dataset
    * You can now provide `dataset` in the hash, or `columns` and `rows`
    * You now no longer need to set additional options on the Array object for local data
* Callbacks are now called with `this` syntax being the DataWorker instance
* Removed `getDataset`. See Upgrade guide for more details
* Now providing distribution files instead of requiring all source files to be included. See the upgrade guide for more details

## v3.x Upgrade Guide

#### Use the new, concatenated file

Rather than including four separate source files, you can now include a single file. There are two flavors. For the minified version, use `dist/dataworker.min.js`. Sourcemaps are also provided for debugging convenience. However, for better debugging, you can use the unminified version at `dist/dataworker.js`. It's still possible to include the source files directly, but a new file has been added.

The `dist` folder also contains two flavors of the webworker code. By default, the webworker will try to use a Blob, and fallback to the main file. However, if you include `dataworker.js` as part of a large distribution file, this may not work very well (or at all). You may also run into issues with tools like `require.js`. For these cases, you can now provide the worker source when instantiation DataWorker. This is also handy to cut down on network traffic, as the helper file is smaller than the main distribution file.

```
    var dw = new DataWorker({
        datasource: "http://example.com/mydata",
        workerSource: "/lib/js/dataworker-helper.js", // this will be tried before any other source
        backupWorkerSource: "/other/path/js/dataworker-helper.js", // this will be tried only if the Blob fails
    });
```

#### Convert `getDataset` to `getRows`

* In their simplest forms, `getDataset` and `getRows` take a callback. In this case, they are identical so you just need to rename the function calls. For example:
    * `getDataset(callback)` becomes `getRows(callback)`
* Both functions allowed you to specify which columns (in which order) are passed to the callback. However, `getRows` now lets you specify which row numbers to get as well. If these are not specified, you will get the full dataset, just like `getDataset`. The start and end row numbers go between the callback and the columns. For example:
    * `getDataset(callback, "column_a", "column_b")` becomes `getRows(callback, undefined, undefined, "column_a", "column_b")`
    * `getDataset(callback, [ "column_a", "column_b" ])` becomes `getRows(callback, undefined, undefined, [ "column_a", "column_b" ])`
