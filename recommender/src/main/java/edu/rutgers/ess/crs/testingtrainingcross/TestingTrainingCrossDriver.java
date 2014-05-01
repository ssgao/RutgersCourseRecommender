package edu.rutgers.ess.crs.testingtrainingcross;

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

public class TestingTrainingCrossDriver extends Configured implements Tool {
	private static final String TRAINING_CSV = "training.csv";
	private static final String TESTING_CSV = "testing.csv";
	private static final String OUTPUT_DIRECTORY = "neighbor";
	private static final String MERGED_FILE = "neighbor.csv";

	public int run(final String[] args) throws Exception {
		
		final Configuration conf = new Configuration();
		
		conf.set(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
		conf.set("mapred.textoutputformat.separator", ",");
		
		DistributedCache.addCacheFile(new Path(args[0] + "/" + TESTING_CSV).toUri(), conf);
		
		final Job job = new Job(conf, "CRS - Testing Cross Training -> Correlation/Neighbor");
		
		job.setJarByClass(TestingTrainingCrossMapper.class);
		job.setMapperClass(TestingTrainingCrossMapper.class);
		job.setReducerClass(TestingTrainingCrossReducer.class);
		job.setNumReduceTasks(4);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path[] { new Path(args[0] + "/" + TRAINING_CSV) });
		final Path outPath = new Path(args[1] + "/" + OUTPUT_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outPath);
		
		outPath.getFileSystem(conf).delete(outPath, true);
		
		if (!job.waitForCompletion(true)) {
			return 1;
		}
		
		final Path mergedFile = new Path(args[1] + "/" + MERGED_FILE);
		CSVFileUtil.mergeToLocal(outPath.getFileSystem(conf), outPath, outPath.getFileSystem(conf), mergedFile, conf,
				true, true);
		
		return 0;
	}
}
