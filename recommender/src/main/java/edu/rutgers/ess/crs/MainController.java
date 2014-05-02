package edu.rutgers.ess.crs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.rutgers.ess.crs.formatcourse.FormatCourseDriver;
import edu.rutgers.ess.crs.formatterm.FormatTermDriver;
import edu.rutgers.ess.crs.neighbor.NeighborDriver;
import edu.rutgers.ess.crs.recommendation.RecommendationDriver;
import edu.rutgers.ess.crs.split.SplitDriver;

public class MainController {
	
	public static void main(final String[] args) throws Exception {
		
		
//		run(new FormatCourseDriver(), appendElement(args, "")); // course.csv
//		run(new FormatTermDriver(), args); // term.csv
//		run(new TermCourseJoinDriver(), args); // test/*.csv, training.csv
//		run(new TestingTrainingCrossDriver(), args); // neighbor.csv, neighbor_ruid_only.csv 
//		run(new FormatCourseDriver(), appendElement(args, "true")); // course2.csv
		run(new RecommendationDriver(), args); // recommendation.csv
		System.exit(0);
	}
	
	private static void run(Tool tool, String args[]) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), tool, args);
		if (res != 0) {
			System.exit(res);
		}
	}
	
	private static String[] appendElement(String[] args, String element) {
		String[] result = new String[args.length + 1];
		for (int i = 0; i < args.length; i++) {
			result[i] = args[i];
		}
		result[args.length] = element;
		return result;
	}
}
