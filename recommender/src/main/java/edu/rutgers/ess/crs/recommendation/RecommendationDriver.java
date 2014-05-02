package edu.rutgers.ess.crs.recommendation;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.rutgers.ess.crs.utility.CSVFileUtil;
import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;

public class RecommendationDriver extends Configured implements Tool {
	static final String COURSE_CSV = "course2.csv";
	static final String RUID_PAIR_CSV = "neighbor_ruid_only.csv";
	static final String TEST_MAJOR_CSV = "198.csv";
	static final String OUTPUT_DIRECTORY = "recommendation";
	static final String MERGED_FILE = "recommendation.csv";

	public int run(final String[] args) throws Exception {

		final Configuration conf = this.getConf();

		conf.set(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
		conf.set("mapred.textoutputformat.separator", "\t");

		final Job job = new Job(conf, "CRS - Neighbor Join Course -> Recommendation");

		job.setJarByClass(RecommendationMapper.class);
		job.setMapperClass(RecommendationMapper.class);
		job.setReducerClass(RecommendationReducer.class);
		job.setNumReduceTasks(4);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path[] { new Path(args[0] + Path.SEPARATOR + COURSE_CSV) });
		final Path outPath = new Path(args[1] + Path.SEPARATOR + OUTPUT_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outPath);

		DistributedCache.addCacheFile(new URI(args[0] + Path.SEPARATOR + RUID_PAIR_CSV),
				job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[0] + Path.SEPARATOR + "test" + Path.SEPARATOR + TEST_MAJOR_CSV),
				job.getConfiguration());

		outPath.getFileSystem(job.getConfiguration()).delete(outPath, true);

		if (!job.waitForCompletion(true)) {
			return 1;
		}

		final Path mergedFile = new Path(args[1] + "/" + MERGED_FILE);
		CSVFileUtil.mergeToLocal(outPath.getFileSystem(job.getConfiguration()), outPath,
				outPath.getFileSystem(job.getConfiguration()), mergedFile, job.getConfiguration(), true, true);

		return 0;
	}
}
