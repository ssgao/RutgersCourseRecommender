package edu.rutgers.ess.crs.neighbor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.rutgers.ess.crs.utility.KeyValueCSVInputFormat;

public class NeighborReducer extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;
	public static String COMPLETE_OUTPUT_PATH = "neighbor.output.path.complete";
	public static String RUID_ONLY_OUTPUT_PATH = "neighbor.output.path.ruidonly";

	protected void setup(Context context) throws IOException {
		this.mos = new MultipleOutputs<Text, Text>(context);
	}

	public void reduce(final Text key, final Iterable<Text> values, Context context) throws IOException,
			InterruptedException {

		final List<String> vals = new ArrayList<String>();
		for (final Text t : values) {
			vals.add(t.toString());
		}

		Collections.sort(vals, Collections.reverseOrder());

		final Configuration conf = context.getConfiguration();
		char dem = conf.get(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",").charAt(0);

		for (int i = 0; i < 50 && i < vals.size(); ++i) {
			String record = vals.get(i);
			int firstDemIndex = record.indexOf(dem);
			int secondDemIndex = record.indexOf(dem, firstDemIndex + 1);
			this.mos.write(key, new Text(record.substring(firstDemIndex + 1, secondDemIndex)),
					conf.get(RUID_ONLY_OUTPUT_PATH) + "/part");
			this.mos.write(key, new Text(record), conf.get(COMPLETE_OUTPUT_PATH) + "/part");
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		this.mos.close();
	}
}
