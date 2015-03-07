module.exports = function(grunt) {
    grunt.initConfig({
        pkg: grunt.file.readJSON("package.json"),
        banner: "/*!\n" +
                " * DataWorker v<%= pkg.version %> (<%= pkg.homepage %>)\n" +
                " * Copyright 2014-<%= grunt.template.today(\"yyyy\") %> <%= pkg.author %>\n" +
                " * Licensed under <%= pkg.license.type %> (<%= pkg.license.url %>)\n" +
                " */\n",

        concat: {
            options: {
                banner: "<%= banner %>",
                stripBanners: false,
                process: function (src, filepath) {
                    if (filepath === "lib/document.currentScript/dist/document.currentScript.js") {
                        return src.replace("{", "{\n" +
                            "\"use strict\";\n\n" +
                            "if (typeof window === \"undefined\") { return; }"
                        );
                    } else {
                        return src;
                    }
                },
            },
            dataworker: {
                src: [
                    "src/webworker-pool.js",
                    "src/actionqueue.js",
                    "src/dw-helper.js",
                    "lib/document.currentScript/dist/document.currentScript.js",
                    "src/dw.js"
                ],
                dest: "dist/<%= pkg.name %>.js"
            },
            helper: {
                src: [ "src/dw-helper.js" ],
                dest: "dist/<%= pkg.name %>-helper.js"
            }
        },
        jshint: {
            beforeconcat: [ "src/*.js" ],
            afterconcat: [ "dist/dataworker.js", "dist/dataworker-helper.js" ],
            test: [ "test/**/*.js" ]
        },
        uglify: {
            options: {
                preserveComments: "some",
                sourceMap: true
            },
            build: {
                files: {
                    "dist/<%= pkg.name %>.min.js": [ "<%= concat.dataworker.dest %>" ],
                    "dist/<%= pkg.name %>-helper.min.js": [ "<%= concat.helper.dest %>" ]
                }
            }
        },
        watch: {
            files: [ "src/*.js" ],
            tasks: [ "dist" ],
        }
    });

    grunt.loadNpmTasks("grunt-contrib-concat");
    grunt.loadNpmTasks("grunt-contrib-uglify");
    grunt.loadNpmTasks("grunt-contrib-watch");
    grunt.loadNpmTasks("grunt-contrib-jshint");

    grunt.registerTask("dist", [ "concat", "uglify" ]);
    grunt.registerTask("test", [
        "jshint:test",
        "jshint:beforeconcat",
        "dist",
        "jshint:afterconcat"
    ]);
};
