package edu.rutgers.ess.crs.neighbor;

import edu.rutgers.ess.crs.utility.CSVFileUtil;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

public class NeighborDriver extends Configured implements Tool {
	static final String TRAINING_CSV = "training.csv";
	static final String TESTING_CSV = "test/198.csv";
	static final String OUTPUT_BASE_DIRECTORY = "cross";
	static final String COMPLETE_OUTPUT_DIRECTORY = "complete";
	static final String COMPLETE_MERGED_FILE = "neighbor.csv";
	static final String RUID_ONLY_OUTPUT_DIRECTORY = "ruid_only";
	static final String RUID_ONLY_MERGED_FILE = "neighbor_ruid_only.csv";

	public int run(final String[] args) throws Exception {

		final Configuration conf = this.getConf();

		conf.set(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set(NeighborReducer.COMPLETE_OUTPUT_PATH, COMPLETE_OUTPUT_DIRECTORY);
		conf.set(NeighborReducer.RUID_ONLY_OUTPUT_PATH, RUID_ONLY_OUTPUT_DIRECTORY);

		DistributedCache.addCacheFile(new Path(args[0] + Path.SEPARATOR + TESTING_CSV).toUri(), conf);

		final Job job = new Job(conf, "CRS - Testing Cross Training -> Correlation/Neighbor");

		job.setJarByClass(NeighborMapper.class);
		job.setMapperClass(NeighborMapper.class);
		job.setReducerClass(NeighborReducer.class);
		job.setNumReduceTasks(4);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path[] { new Path(args[0] + Path.SEPARATOR + TRAINING_CSV) });
		final Path outFullPath = new Path(args[1] + Path.SEPARATOR + OUTPUT_BASE_DIRECTORY + Path.SEPARATOR + COMPLETE_OUTPUT_DIRECTORY);
		final Path outRuidOnlyPath = new Path(args[1] + Path.SEPARATOR + OUTPUT_BASE_DIRECTORY + Path.SEPARATOR + RUID_ONLY_OUTPUT_DIRECTORY);
		final Path outBasePath = new Path(args[1] + Path.SEPARATOR + OUTPUT_BASE_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outBasePath);

		outBasePath.getFileSystem(conf).delete(outBasePath, true);
		outFullPath.getFileSystem(conf).delete(outFullPath, true);
		outRuidOnlyPath.getFileSystem(conf).delete(outRuidOnlyPath, true);

		if (!job.waitForCompletion(true)) {
			return 1;
		}

		final Path mergedFullFile = new Path(args[1] + Path.SEPARATOR + COMPLETE_MERGED_FILE);
		CSVFileUtil.mergeToLocal(outFullPath.getFileSystem(conf), outFullPath, outFullPath.getFileSystem(conf),
				mergedFullFile, conf, true, true);

		final Path mergedRuidOnlyFile = new Path(args[1] + Path.SEPARATOR + RUID_ONLY_MERGED_FILE);
		CSVFileUtil.mergeToLocal(outRuidOnlyPath.getFileSystem(conf), outRuidOnlyPath,
				outRuidOnlyPath.getFileSystem(conf), mergedRuidOnlyFile, conf, true, true);

		outBasePath.getFileSystem(conf).delete(outBasePath, true);
		return 0;
	}
}
