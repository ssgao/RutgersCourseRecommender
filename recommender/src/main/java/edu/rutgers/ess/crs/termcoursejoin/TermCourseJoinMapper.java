package edu.rutgers.ess.crs.termcoursejoin;

import org.apache.hadoop.io.Writable;
import java.util.ArrayList;
import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.concurrent.ConcurrentHashMap;
import edu.rutgers.ess.crs.utility.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TermCourseJoinMapper extends Mapper<Text, TextArrayWritable, Text, TextArrayWritable> {
	
	private ConcurrentHashMap<Text, String[]> studentMap = new ConcurrentHashMap<Text, String[]>();
	private MultipleOutputs<Text, TextArrayWritable> mos;
	public static String TRAINING_OUTPUT_PATH = "termcoursejoinmapper.output.path.training";
	public static String TESTING_OUTPUT_PATH = "termcoursejoinmapper.output.path.testing";

	protected void setup(Context context) throws IOException {
		
		Configuration conf = context.getConfiguration();
		
		try {
			
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			
			if (cacheFiles != null && cacheFiles.length == 1) {
				this.processFile(cacheFiles[0], conf);
			}
		} catch (IOException e) {
			System.err.println("error reading distributedCache: " + e);
		}
		
		this.mos = new MultipleOutputs<Text, TextArrayWritable>(context);
	}

	private void processFile(final Path path, final Configuration conf) {
		
		BufferedReader br = null;
		try {
			FileReader fr = new FileReader(path.toString());
			br = new BufferedReader(fr);
			this.populateMap(br, conf);
		} catch (FileNotFoundException e) {
			
			System.err.println("file: " + path.toString() + " is missing");
		} finally {
			
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException ex) {
			}
		}
	}

	private void populateMap(final BufferedReader br, final Configuration conf) {
		
		String line = null;
		String[] tokens = null;
		String[] studentRecord = null;
		
		try {
			while ((line = br.readLine()) != null) {
				tokens = line.split(conf.get(KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";"), 2);
				studentRecord = tokens[1].split(conf.get(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ","), 5);
				this.studentMap.put(new Text(tokens[0]), studentRecord);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	protected void map(final Text key, final TextArrayWritable value, Context context) throws IOException,
			InterruptedException {
		
		final String[] studentRecord = this.studentMap.get(key);
		if (studentRecord == null) {
			return;
		}
		final ArrayList<Text> result = new ArrayList<Text>();
		for (int i = 0; i < studentRecord.length; ++i) {
			result.add(new Text(studentRecord[i]));
		}
		for (final Writable t : value.get()) {
			result.add((Text) t);
		}
		final Configuration conf = context.getConfiguration();
		if (studentRecord[1].compareTo("20140") > 0) {
			this.mos.write(key, new TextArrayWritable(result.toArray(new Text[0])),
					conf.get(TermCourseJoinMapper.TESTING_OUTPUT_PATH));
		}
		this.mos.write(key, new TextArrayWritable(result.toArray(new Text[0])),
				conf.get(TermCourseJoinMapper.TRAINING_OUTPUT_PATH));
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		this.mos.close();
	}
}
