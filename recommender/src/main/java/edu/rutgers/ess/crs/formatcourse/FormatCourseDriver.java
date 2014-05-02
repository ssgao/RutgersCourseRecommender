package edu.rutgers.ess.crs.formatcourse;

import org.apache.hadoop.conf.Configuration;
import edu.rutgers.ess.crs.utility.CSVFileUtil;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import edu.rutgers.ess.crs.utility.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import edu.rutgers.ess.crs.utility.KeyValueCSVOutputFormat;
import edu.rutgers.ess.crs.utility.CSVInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

public class FormatCourseDriver extends Configured implements Tool {
	private static final String RAW_COURSE_CSV = "raw/course_export.csv";
	private static final String OUPUT_DIRECTORY = "course";
	private static final String MERGED_SIMPLE_FILE = "course.csv";
	private static final String MERGED_COMPLETE_FILE = "course2.csv";

	/**
	 * args[0] input base directory args[1] output base directory args[2] include ytg
	 */
	public int run(final String[] args) throws Exception {

		final Configuration conf = this.getConf();

		conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.setBoolean(FormatCourseMapper.INCLUDE_YEAR_TERM_GRADE, Boolean.parseBoolean(args[2]));
		conf.set(KeyValueCSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");

		final Job job = new Job(conf, "CRS - Course Data Transformation");

		job.setJarByClass(FormatCourseMapper.class);
		job.setMapperClass(FormatCourseMapper.class);
		job.setReducerClass(FormatCourseReducer.class);
		job.setNumReduceTasks(4);

		job.setInputFormatClass(CSVInputFormat.class);
		job.setOutputFormatClass(KeyValueCSVOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);

		final Path inPath = new Path(args[0] + Path.SEPARATOR + RAW_COURSE_CSV);
		FileInputFormat.setInputPaths(job, new Path[] { inPath });
		final Path outPath = new Path(args[1] + Path.SEPARATOR + OUPUT_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outPath);

		// delete the directory is it's there
		outPath.getFileSystem(conf).delete(outPath, true);

		if (!job.waitForCompletion(true)) {
			return 1;
		}

		final Path mergedPath;
		if (args[2].equals("true")) {
			mergedPath = new Path(args[1] + Path.SEPARATOR + MERGED_COMPLETE_FILE);
		} else {
			mergedPath = new Path(args[1] + Path.SEPARATOR + MERGED_SIMPLE_FILE);
		}

		CSVFileUtil.mergeToLocal(outPath.getFileSystem(conf), outPath, outPath.getFileSystem(conf), mergedPath, conf,
				true, true);
		return 0;
	}
}
