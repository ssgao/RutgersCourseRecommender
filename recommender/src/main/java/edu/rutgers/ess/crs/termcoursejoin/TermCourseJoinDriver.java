package edu.rutgers.ess.crs.termcoursejoin;

import edu.rutgers.ess.crs.utility.CSVFileUtil;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import edu.rutgers.ess.crs.utility.TextArrayWritable;
import org.apache.hadoop.io.Text;
import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import edu.rutgers.ess.crs.utility.KeyValueCSVOutputFormat;
import edu.rutgers.ess.crs.utility.CSVInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

public class TermCourseJoinDriver extends Configured implements Tool {
	private static final String COURSE_CSV = "course.csv";
	private static final String TERM_CSV = "term.csv";
	private static final String OUTPUT_BASE_DIRECTORY = "split";
	private static final String TRAINING_OUTPUT_DIRECTORY = "training";
	private static final String TRAINING_MERGED_FILE = "training.csv";
	private static final String TESTING_OUTPUT_DIRECTORY = "testing";
	private static final String TESTING_MERGED_FILE = "testing.csv";

	public int run(final String[] args) throws Exception {

		final Configuration conf = new Configuration();

		conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
		conf.set(TermCourseJoinMapper.TRAINING_OUTPUT_PATH, "training/part");
		conf.set(TermCourseJoinMapper.TESTING_OUTPUT_PATH, "testing/part");

		DistributedCache.addCacheFile(new Path(args[0] + "/" + TERM_CSV).toUri(), conf);

		final Job job = new Job(conf, "CRS - Term Join Course -> TRAINING/TESTING");

		job.setJarByClass(TermCourseJoinMapper.class);
		job.setMapperClass(TermCourseJoinMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(KeyValueCSVInputFormat.class);
		job.setOutputFormatClass(KeyValueCSVOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		FileInputFormat.setInputPaths(job, new Path[] { new Path(args[0] + "/" + COURSE_CSV) });
		final Path outTrainingPath = new Path(args[1] + "/" + OUTPUT_BASE_DIRECTORY + "/" + TRAINING_OUTPUT_DIRECTORY);
		final Path outTestingPath = new Path(args[1] + "/" + OUTPUT_BASE_DIRECTORY + "/" + TESTING_OUTPUT_DIRECTORY);
		final Path outBasePath = new Path(args[1] + "/" + OUTPUT_BASE_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outBasePath);

		outBasePath.getFileSystem(conf).delete(outBasePath, true);
		outTrainingPath.getFileSystem(conf).delete(outTrainingPath, true);
		outTestingPath.getFileSystem(conf).delete(outTestingPath, true);

		if (!job.waitForCompletion(true)) {
			return 1;
		}

		final Path mergedTrainingPath = new Path(args[1] + "/" + TRAINING_MERGED_FILE);
		CSVFileUtil.mergeToLocal(outTrainingPath.getFileSystem(conf), outTrainingPath,
				outTrainingPath.getFileSystem(conf), mergedTrainingPath, conf, true, true);

		final Path mergedTestingPath = new Path(args[1] + "/" + TESTING_MERGED_FILE);
		CSVFileUtil.mergeToLocal(outTestingPath.getFileSystem(conf), outTestingPath,
				outTestingPath.getFileSystem(conf), mergedTestingPath, conf, true, true);

		outBasePath.getFileSystem(conf).delete(outBasePath, true);
		return 0;
	}
}
