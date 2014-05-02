package edu.rutgers.ess.crs.split;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.rutgers.ess.crs.utility.CSVFileUtil;
import edu.rutgers.ess.crs.utility.CSVInputFormat;
import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;
import edu.rutgers.ess.crs.utility.KeyValueCSVOutputFormat;
import edu.rutgers.ess.crs.utility.TextArrayWritable;

public class SplitDriver extends Configured implements Tool {
	private static final String COURSE_CSV = "course.csv";
	private static final String TERM_CSV = "term.csv";
	private static final String OUTPUT_BASE_DIRECTORY = "split";
	private static final String TRAINING_OUTPUT_DIRECTORY = "training";
	private static final String TRAINING_MERGED_FILE = "training.csv";
	private static final String TESTING_OUTPUT_DIRECTORY = "testing";
	private static final String TESTING_MERGED_DIRECTORY = "test";

	public int run(final String[] args) throws Exception {

		final Configuration conf = this.getConf();

		conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
		conf.set(SplitMapper.TRAINING_OUTPUT_PATH, TRAINING_OUTPUT_DIRECTORY);
		conf.set(SplitMapper.TESTING_OUTPUT_PATH, TESTING_OUTPUT_DIRECTORY);

		DistributedCache.addCacheFile(new Path(args[0] + Path.SEPARATOR + TERM_CSV).toUri(), conf);

		final Job job = new Job(conf, "CRS - Term Join Course -> TRAINING/TESTING");

		job.setJarByClass(SplitMapper.class);
		job.setMapperClass(SplitMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(KeyValueCSVInputFormat.class);
		job.setOutputFormatClass(KeyValueCSVOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		FileInputFormat.setInputPaths(job, new Path[] { new Path(args[0] + Path.SEPARATOR + COURSE_CSV) });
		final Path outTrainingPath = new Path(args[1] + Path.SEPARATOR + OUTPUT_BASE_DIRECTORY + Path.SEPARATOR + TRAINING_OUTPUT_DIRECTORY);
		final Path outTestingPath = new Path(args[1] + Path.SEPARATOR + OUTPUT_BASE_DIRECTORY + Path.SEPARATOR + TESTING_OUTPUT_DIRECTORY);
		final Path outBasePath = new Path(args[1] + Path.SEPARATOR + OUTPUT_BASE_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outBasePath);

		outBasePath.getFileSystem(conf).delete(outBasePath, true);
		outTrainingPath.getFileSystem(conf).delete(outTrainingPath, true);
		outTestingPath.getFileSystem(conf).delete(outTestingPath, true);

		if (!job.waitForCompletion(true)) {
			return 1;
		}

		final Path mergedTrainingPath = new Path(args[1] + Path.SEPARATOR + TRAINING_MERGED_FILE);
		CSVFileUtil.mergeToLocal(outTrainingPath.getFileSystem(conf), outTrainingPath,
				outTrainingPath.getFileSystem(conf), mergedTrainingPath, conf, true, true);

		final Path mergedTestingPath = new Path(args[1] + Path.SEPARATOR + TESTING_MERGED_DIRECTORY);
		processTestDirectories(outTestingPath.getFileSystem(conf), outTestingPath, mergedTestingPath, conf);

		outBasePath.getFileSystem(conf).delete(outBasePath, true);
		return 0;
	}

	private void processTestDirectories(final FileSystem fs, final Path src, final Path dst, final Configuration conf)
			throws IOException {
		FileStatus[] list = fs.listStatus(src);
		for (FileStatus dir : list) {
			if (dir.isDir()) {
				Path path = dir.getPath();
				String dirName = path.getName();
				final Path mergedMajorPath = new Path(dst + Path.SEPARATOR + dirName + ".csv");
				CSVFileUtil.mergeToLocal(fs, path, fs, mergedMajorPath, conf, true, true);
			}
		}
	}
}
