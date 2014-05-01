package edu.rutgers.ess.crs.formatterm;

import java.io.IOException;
import java.util.Set;
import java.util.Iterator;
import org.apache.hadoop.io.Writable;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import edu.rutgers.ess.crs.utility.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FormatTermReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
	public void reduce(final Text key, final Iterable<TextArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		final ArrayList<String> yts = new ArrayList<String>();
		Writable[] termRecord = null;
		final Iterator<TextArrayWritable> i = values.iterator();
		while (i.hasNext()) {
			termRecord = i.next().get();
			final String yt = ((Text) termRecord[0]).toString() + ((Text) termRecord[1]).toString() + ","
					+ ((Text) termRecord[2]).toString() + "," + ((Text) termRecord[3]).toString() + ","
					+ ((Text) termRecord[4]).toString();
			yts.add(yt);
		}
		
		Collections.sort(yts);
		
		final String[] firstYt = yts.get(0).split(",");
		final Text admitYt = new Text(firstYt[0]);
		final String[] lastYt = yts.get(yts.size() - 1).split(",", -1);
		final Text lastEnrolledYt = new Text(lastYt[0]);
		final Set<String> excludedMajors = new HashSet<String>(Arrays.asList("000", "001", "004", "006", "007", "150",
				"155", "180", "540"));
		
		if (lastYt[1].length() == 0 || lastYt[1].charAt(0) == ' ' || excludedMajors.contains(lastYt[1])) {
			lastYt[1] = "";
		}
		final TextArrayWritable outputValue = new TextArrayWritable(new Text[] { admitYt, lastEnrolledYt,
				new Text(lastYt[1]), new Text(lastYt[2]), new Text(lastYt[3]) });
		
		context.write(key, outputValue);
	}
}
