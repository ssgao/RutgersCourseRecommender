package edu.rutgers.ess.crs.formatterm;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import edu.rutgers.ess.crs.utility.TextArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FormatTermMapper extends Mapper<LongWritable, TextArrayWritable, Text, TextArrayWritable> {
	protected void map(final LongWritable key, final TextArrayWritable value, Context context) throws IOException,
			InterruptedException {
		final Writable[] vals = value.get();
		final Text outputKey = (Text) vals[17];
		final TextArrayWritable outputValue = new TextArrayWritable(new Text[] { (Text) vals[0], (Text) vals[1],
				(Text) vals[2], (Text) vals[6], (Text) vals[13] });
		context.write(outputKey, outputValue);
	}
}
