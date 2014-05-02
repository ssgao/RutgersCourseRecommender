package edu.rutgers.ess.crs.recommendation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.rutgers.ess.crs.utility.Grade;
import edu.rutgers.ess.crs.utility.YearTermOfStudy;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {

	private static final String YEAR = "2014"; // current year of interest
	private static final String TERM = "1"; // current term of interest

	/**
	 * Test(my) student record
	 * 
	 * @author simongao
	 */
	private static class Student {
		String admitYear; // year student is admitted
		String admitTerm; // term student is admitted
		Set<String> courseSet; // set of courses this student has taken

		public Student(String admitYear, String admitTerm, String record) {
			this.admitYear = admitYear;
			this.admitTerm = admitTerm;
			populateSet(record);
		}

		/**
		 * Assuming input is from test/*.csv, a record would look like: 154006071;20139,20149,332,,,01:160:159,01:160:171,01:220:102,01:220:103,01:355:100....
		 * ruid;admitYt,currentYt,major,major2,minor,[courses]
		 * 
		 * @param record
		 */
		private void populateSet(String record) {
			String[] records = record.split(",", -1);
			this.courseSet = new HashSet<String>(records.length - 5);
			for (int i = 5; i < records.length; i++) {
				this.courseSet.add(records[i]);
			}
		}
	}

	/**
	 * A course that may recommend to me (test)
	 * 
	 * @author simongao
	 */
	private static class Recommendation {
		int studentTaken;
		double totalGrade;
		String courseId;
		int totalStudent;
		String courseTitle;

		/**
		 * output should take the format of course id,average grade,percent taken e.g. 01:156:423:3.5:0.4
		 */
		public String toString() {
			double averageGrade = totalGrade / studentTaken;
			double percentTaken = (double) studentTaken / totalStudent;
			return courseId + "\t" + String.format("%.2f", averageGrade) + "\t" + String.format("%.2f", percentTaken) + "\t" + courseTitle;
		}
	}

	/**
	 * key = ruid value = a student object
	 */
	private Map<String, Student> recordMap = new HashMap<String, Student>();

	/**
	 * retrieve the test/*.csv file
	 */
	protected void setup(Context context) throws IOException {
		final Configuration conf = context.getConfiguration();
		try {
			final Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			if (cacheFiles != null && cacheFiles.length > 0) {
				for (Path p : cacheFiles) {
					if (p.getName().equals(RecommendationDriver.TEST_MAJOR_CSV)) {
						this.processFile(p, conf);
						return;
					}
				}
			}
			throw new IOException(RecommendationDriver.TEST_MAJOR_CSV + " missing");
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

		try {
			while ((line = br.readLine()) != null) {
				// first 9 characters are ruid
				String id = line.substring(0, 9);
				// followed by a ; then 4 digit year
				String year = line.substring(10, 14);
				// then 1 digit term
				String term = line.substring(14, 15);
				this.recordMap.put(id, new Student(year, term, line));
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Construct the list of recommended courses from input list for input key, which is test(my) ruid
	 */
	public void reduce(final Text key, final Iterable<Text> values, Context context) throws IOException,
			InterruptedException {

		String myRuid = key.toString();
		
		// find me on recordMap, which is preprocessed during reduce job setup
		Student me = recordMap.get(myRuid);

		// find my current year term of study based on my admit year/term and current year/term
		YearTermOfStudy myYearTermOfStudy = YearTermOfStudy.transform(me.admitYear, me.admitTerm, YEAR, TERM);
		
		// key-> course id, value -> recommendation object
		Map<String, Recommendation> rMap = new HashMap<String, Recommendation>();

		int neighborCounter = 0; // a counter used for checking how many neighbors there are for me

		// process list of courses sent from each of my neighbor
		for (Text val : values) {
			// course is separated by ,
			String[] courses = val.toString().split(",");
			// the first 4 digit are year
			String firstYear = courses[0].substring(0, 4);
			// 5th digit is term
			String firstTerm = courses[0].substring(4, 5);

			// find the desired year/term of my neighbor based on his/her first year/term and my yeartermofstudy
			String desiredYearTerm = YearTermOfStudy.transform(firstYear, firstTerm, myYearTermOfStudy);

			// go through the list of my neighbor's course
			for (String course : courses) {
				// locate the desired year/term, each record is structured like:
				// yearterm/courseid/grade. e.g. 20111/01:241:231/A
				if (course.substring(0, 5).equals(desiredYearTerm)) {
					String[] courseInfo = course.split("###", -1);

					// find if this course is exist in our map, if not, create it
					Recommendation r = rMap.get(courseInfo[1]);
					if (r == null) {
						r = new Recommendation();
						r.courseId = courseInfo[1];
						r.courseTitle = courseInfo[3];
						rMap.put(courseInfo[1], r);
					}
					// update the record
					r.totalGrade += Grade.fromString(courseInfo[2]).getGradePoints();
					r.studentTaken++;

				}
			}
			neighborCounter++;
		}

		// get all the courses my neighbors toke
		Collection<Recommendation> rCollection = rMap.values();
		for (Recommendation r : rCollection) {
			// find if I already took this course
			if (!me.courseSet.contains(r.courseId)) {
				// if not, export it
				r.totalStudent = neighborCounter;
				context.write(key, new Text(r.toString()));
			}
		}
	}
}
