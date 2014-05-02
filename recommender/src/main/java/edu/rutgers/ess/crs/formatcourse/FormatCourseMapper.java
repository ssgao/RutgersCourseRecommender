package edu.rutgers.ess.crs.formatcourse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.rutgers.ess.crs.utility.TextArrayWritable;

public class FormatCourseMapper extends Mapper<LongWritable, TextArrayWritable, Text, Text> {
	
	private static boolean ytg;
	public static String INCLUDE_YEAR_TERM_GRADE = "formatcoursemapper.include.yeartermgrade";
	
	protected void setup(Context context) throws IOException {
		
		ytg = context.getConfiguration().getBoolean(INCLUDE_YEAR_TERM_GRADE, false);
	}
	
	protected void map(final LongWritable key, final TextArrayWritable value, Context context) throws IOException,
			InterruptedException {

		final Writable[] vals = value.get();
		final int year = Integer.parseInt(((Text) vals[0]).toString());
		final String offeringUnit = ((Text) vals[2]).toString();
		final String subj = ((Text) vals[4]).toString();
		final String courseNo = ((Text) vals[5]).toString();
		final String courseType = ((Text) vals[13]).toString();
		final String yearterm = ((Text) vals[0]).toString() + ((Text) vals[1]).toString();
		final String grade = ((Text) vals[17]).toString();
		final String title = ((Text) vals[12]).toString().replace(',', ' ');
		
		if (courseType == null) {
			return;
		}

		if (((courseType.equals("INTL") && year < 2014) || courseType.equals("TRAN") || courseType.equals("EXTX"))
				&& offeringUnit != null && offeringUnit.length() != 0 && offeringUnit.charAt(0) != ' ' && subj != null
				&& subj.length() != 0 && subj.charAt(0) != ' ' && courseNo != null && courseNo.length() != 0
				&& courseNo.charAt(0) != ' ') {

			final Text outputKey = (Text) vals[36];
			final Text outputValue;
			if (ytg) {
				outputValue = new Text(yearterm + "###" + offeringUnit + ":" + subj + ":" + courseNo + "###" + grade + "###" + title);
			} else {
				outputValue = new Text(offeringUnit + ":" + subj + ":" + courseNo);
			}
			context.write(outputKey, outputValue);
		}
	}
}
