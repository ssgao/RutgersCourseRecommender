package edu.rutgers.ess.crs.recommendation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommendationMapper extends Mapper<Text, Text, Text, Text> {

	/**
	 *  key = neighbor's(training) ruid, value = list of test(my) ruids
	 */
	private Map<String, LinkedList<String>> testMap = new HashMap<String, LinkedList<String>>();

	/**
	 * Retrive the replicated ruid pair file
	 */
	protected void setup(Context context) throws IOException {
		final Configuration conf = context.getConfiguration();
		try {
			final Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			if (cacheFiles != null && cacheFiles.length > 0) {
				for (Path p : cacheFiles) {
					if (p.getName().equals(RecommendationDriver.RUID_PAIR_CSV)) {
						this.processFile(p, conf);
						return;
					}
				}
			}
			throw new IOException(RecommendationDriver.RUID_PAIR_CSV + " missing");
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

	/**
	 * Construct the map based on the input from file
	 * @param br
	 * @param conf
	 */
	private void populateMap(final BufferedReader br, final Configuration conf) {
		String line = null;

		try {
			while ((line = br.readLine()) != null) {
				String id1 = line.substring(0, 9); // my(test) ruid
				String id2 = line.substring(10, 19); // neighbor's (training) ruid
				LinkedList<String> ll = this.testMap.get(id2); // list of test ruids
				if (ll == null) {
					ll = new LinkedList<String>();
					ll.add(id1);
					this.testMap.put(id2, ll);
				} else {
					ll.add(id1);
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * input = a row in course2.csv file
	 * key = ruid
	 * value = a list of courses with grade&yearterm
	 * 
	 * It checks if this ruid is a training ruid of interest
	 * 		if it is -> for each of it's mapping test(my) ruids:
	 * 						output a key value combination of key = a test ruid
	 * 														  value = the input value
	 */
	protected void map(final Text key, final Text value, Context context) throws IOException, InterruptedException {

		String id = key.toString();
		LinkedList<String> ruids = this.testMap.get(id);
		if (ruids == null || ruids.size() == 0) {
			return;
		}

		for (String ruid : ruids) {
			context.write(new Text(ruid), value);
		}
	}
}
