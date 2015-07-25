var gulp = require('gulp');
var child = require('child_process');
var run = require('gulp-run');
var util = require('gulp-util')

gulp.task('build', function() {
	build = child.spawnSync("go", ["install"])
	if(build.stderr.length)	{
		var lines = build.stderr.toString().split('\n');
		for (var l in lines)
			util.log(util.colors.red(lines[l]));
	}
	util.log(util.colors.green('go build successful'));
	return build;
});

gulp.task('watch', function(){
	gulp.watch('**/*.go', ['build']);
})