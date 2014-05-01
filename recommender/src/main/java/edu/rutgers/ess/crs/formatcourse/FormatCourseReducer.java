package edu.rutgers.ess.crs.formatcourse;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import edu.rutgers.ess.crs.utility.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FormatCourseReducer extends Reducer<Text, Text, Text, TextArrayWritable> {
	public void reduce(final Text key, final Iterable<Text> values, Context context) throws IOException,
			InterruptedException {

		// using treeset so that the output is sorted
		final Set<String> courses = new TreeSet<String>();

		for (final Text t : values) {
			courses.add(t.toString());
		}

		final Text[] tArray = new Text[courses.size()];

		final Iterator<String> it = courses.iterator();
		int i = 0;
		while (it.hasNext()) {
			tArray[i++] = new Text((String) it.next());
		}
		
		final TextArrayWritable outputValue = new TextArrayWritable(tArray);
		context.write(key, outputValue);
	}
}
