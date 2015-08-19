var gulp = require('gulp');
// requires node version >= 12
var child = require('child_process');
var run = require('gulp-run');
var chalk = require('chalk')
var util = require('gulp-util')

var gulpProcess;
   
gulp.task('build', function() {
  build = child.spawnSync("go", ["install"])
  if(build.stderr.length) {
    util.log(chalk.white.bgRed.bold(" GO Install Failed "))
    var lines = build.stderr.toString().split('\n');
    for (var l in lines)
      util.log(chalk.red(lines[l]));
  } else {
    util.log(chalk.white.bgGreen.bold(' Go Install Successful '));
  }
  return build;
});

gulp.task('watch', function(){
  gulp.watch('**/*.go', ['build']);
})

// Variety of ways explored to get gulp to reload on gulpfile edit.
// In the end went with gulper: npm install -g gulper.
// I can't figure out how to turn off notificationn, but it's a small
// price.
