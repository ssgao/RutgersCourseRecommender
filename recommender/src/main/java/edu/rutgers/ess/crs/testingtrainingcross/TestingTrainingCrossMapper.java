package edu.rutgers.ess.crs.testingtrainingcross;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;

public class TestingTrainingCrossMapper extends Mapper<Text, Text, Text, Text> {
	private List<String[]> testingList;

	public TestingTrainingCrossMapper() {
		super();
		this.testingList = new LinkedList<String[]>();
	}

	protected void setup(Context context) throws IOException {
		final Configuration conf = context.getConfiguration();
		try {
			final Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			if (cacheFiles != null && cacheFiles.length == 1) {
				this.processFile(cacheFiles[0], conf);
			}
		} catch (IOException e) {
			System.err.println("error reading distributedCache: " + e);
		}
	}

	private void processFile(final Path path, final Configuration conf) {
		BufferedReader br = null;
		try {
			final FileReader fr = new FileReader(path.toString());
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
		String[] studentRecord = null;
		try {
			while ((line = br.readLine()) != null) {
				final String dem1 = conf.get(KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG, ";");
				final String dem2 = conf.get(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
				studentRecord = line.split(dem1 + "|" + dem2, -1);
				this.testingList.add(studentRecord);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	protected void map(final Text key, final Text value, Context context) throws IOException, InterruptedException {

		final String ruid = key.toString();

		final String[] trainingRecord = value.toString().split(
				context.getConfiguration().get(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ","), -1);

		final boolean hasMajor = trainingRecord[2].length() != 0;
		final boolean hasMajor2 = trainingRecord[3].length() != 0;

		for (final String[] testingRecord : this.testingList) {

			if (ruid.equals(testingRecord[0])) {
				continue;
			}

			final ArrayList<Text> common = new ArrayList<Text>();

			common.add(new Text("0")); // placeholder for rating
			common.add(new Text(ruid)); // ruid

			double rating = 0.0;
			if (hasMajor) {
				if (testingRecord[3].equals(trainingRecord[2])) {
					rating += 0.2;
				}
				if (testingRecord[3].equals(trainingRecord[3])) {
					rating += 0.2;
				}
			}

			if (hasMajor2) {
				if (testingRecord[4].equals(trainingRecord[2])) {
					rating += 0.2;
				}
				if (testingRecord[4].equals(trainingRecord[3])) {
					rating += 0.2;
				}
			}

			common.add(new Text(String.valueOf((int) (rating / 0.2)))); // # of major in common

			if (testingRecord[5].length() != 0 && testingRecord[5].equals(trainingRecord[4])) {
				common.add(new Text("1")); // # of minor in common
				rating += 0.05;
			} else {
				common.add(new Text("0")); // # of minor in common
			}

			common.add(new Text("0")); // placeholder for # of courses in common

			int iTe = 6; // starting index for courses on testing record
			int iTr = 5; // starting index for courses on training record

			final int sTe = testingRecord.length;
			final int sTr = trainingRecord.length;

			// find intersection of courses
			while (iTe < sTe && iTr < sTr) {
				final int score = testingRecord[iTe].compareTo(trainingRecord[iTr]);
				if (score < 0) {
					++iTe;
				} else if (score > 0) {
					++iTr;
				} else {
					common.add(new Text(testingRecord[iTe]));
					++iTe;
					++iTr;
				}
			}

			rating += (common.size() - 5) / 40.0 * 0.55;

			common.set(0, new Text(String.valueOf(rating))); // update rating
			common.set(4, new Text(String.valueOf(common.size() - 5))); // updating # of courses in common
			
			if (rating <= 0.0) { // ignore ratings that are 0
				continue;
			}
			
			final Text outputKey = new Text(testingRecord[0]);
			final Text outputValue = new Text(StringUtils.join(common, ','));
			
			context.write(outputKey, outputValue);
		}
	}
}
