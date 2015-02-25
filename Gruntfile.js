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
                stripBanners: false
            },
            dataworker: {
                src: [
                    "src/webworker-pool.js",
                    "src/actionqueue.js",
                    "src/dw-helper.js",
                    "src/dw.js"
                ],
                dest: "dist/<%= pkg.name %>.js"
            },
            helper: {
                src: [ "src/dw-helper.js" ],
                dest: "dist/<%= pkg.name %>-helper.js"
            }
        },
        uglify: {
            options: {
                preserveComments: "some"
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

    grunt.registerTask("dist", [ "concat", "uglify" ]);
};
