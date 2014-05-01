package edu.rutgers.ess.crs.formatterm;

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

public class FormatTermDriver extends Configured implements Tool {
	private static final String RAW_CSV = "raw/term_export.csv";
	private static final String OUPUT_DIRECTORY = "term";
	private static final String MERGED_FILE = "term.csv";

	public int run(final String[] args) throws Exception {
		
		final Configuration conf = this.getConf();
		
		conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
		conf.set(KeyValueCSVOutputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
		
		final Job job = new Job(conf, "CRS - Term Data Transformation");
		
		job.setJarByClass(FormatTermMapper.class);
		job.setMapperClass(FormatTermMapper.class);
		job.setReducerClass(FormatTermReducer.class);
		job.setNumReduceTasks(4);
		
		job.setInputFormatClass(CSVInputFormat.class);
		job.setOutputFormatClass(KeyValueCSVOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);
		
		final Path inPath = new Path(args[0] + "/" + RAW_CSV);
		FileInputFormat.setInputPaths(job, new Path[] { inPath });
		final Path outPath = new Path(args[1] + "/" + OUPUT_DIRECTORY);
		FileOutputFormat.setOutputPath(job, outPath);
		
		outPath.getFileSystem(conf).delete(outPath, true);
		
		if (!job.waitForCompletion(true)) {
			return 1;
		}
		
		final Path mergedPath = new Path(args[1] + "/" + MERGED_FILE);
		CSVFileUtil.mergeToLocal(outPath.getFileSystem(conf), outPath, outPath.getFileSystem(conf), mergedPath, conf,
				true, true);
		return 0;
	}
}
