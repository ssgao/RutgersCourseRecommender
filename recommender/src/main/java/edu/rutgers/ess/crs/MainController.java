package edu.rutgers.ess.crs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.rutgers.ess.crs.formatcourse.FormatCourseDriver;
import edu.rutgers.ess.crs.formatterm.FormatTermDriver;
import edu.rutgers.ess.crs.termcoursejoin.TermCourseJoinDriver;
import edu.rutgers.ess.crs.testingtrainingcross.TestingTrainingCrossDriver;

public class MainController {
	
	public static void main(final String[] args) throws Exception {
		
		run(new FormatCourseDriver(), args);
		run(new FormatTermDriver(), args);
		run(new TermCourseJoinDriver(), args);
		run(new TestingTrainingCrossDriver(), args);
		System.exit(0);
	}
	
	private static void run(Tool tool, String args[]) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), tool, args);
		if (res != 0) {
			System.exit(res);
		}
	}
}
