package edu.rutgers.ess.crs.utility;

public enum YearTermOfStudy {

	FreshmenFirst(1, "First Semester of Freshmen"),
	FreshmenSecond(2, "Second Semester of Freshmen"),
	SophomoreFirst(3, "First Semester of Sophomore"),
	SophomoreSecond(4, "Second Semester of Sophomore"),
	JuniorFirst(5, "First Semester of Junior"),
	JuniorSecond(6, "Second Semester of Junior"),
	SeniorFirst(7, "First Semester of Senior"),
	SeniorSecond(8, "Second Semester of Senior"),
	NULL(-1, "IllegalArgument");
	
	private final int weight;
	private final String asString;
	
	private YearTermOfStudy(int weight, String asString) {
	
		this.weight = weight;
		this.asString = asString;
	}
	
	public int getWeight() {
		
		return this.weight;
	}
	
	/**
	 * Transform the admit year & term with current year & term into YearTermOfStudy enum
	 * @param ay admit year
	 * @param at admit term
	 * @param cy current year
	 * @param ct current term
	 * @return
	 */
	public static YearTermOfStudy transform(String ay, String at, String cy, String ct) {
		
		int admitYear = Integer.parseInt(ay);
		int admitTerm = Integer.parseInt(at);
		int courseTakenYear = Integer.parseInt(cy);
		int courseTakenTerm = Integer.parseInt(ct);
		
		// change to closest spring/fall term
		admitTerm = admitTerm >= 7 ? 9 : 1;
		
		int admitYearTerm = admitYear * 10 + admitTerm;
		int courseTakenYearTerm = courseTakenYear * 10 + courseTakenTerm;
		switch(courseTakenYearTerm - admitYearTerm) {
		case -2: case -1: case  0: case  6: return FreshmenFirst;
		case  2: case  8: 					return FreshmenSecond;
		case  9: case 10: case 16: 			return SophomoreFirst;
		case 12: case 18: 					return SophomoreSecond;
		case 19: case 20: case 26:			return JuniorFirst;
		case 22: case 28: 					return JuniorSecond;
		case 29: case 30: case 36:			return SeniorFirst;
		case 32: case 38: 					return SeniorSecond;
		default: 							return NULL;
		}
	}
	
	/**
	 * Given admit year, admit term and desired YearTermOfStudy, calculate desired year and term
	 */
	public static String transform(String ay, String at, YearTermOfStudy ytos) {
		
		int admitYear = Integer.parseInt(ay);
		int admitTerm = Integer.parseInt(at);
		
		// change to closest spring/fall term
		admitTerm = admitTerm >= 7 ? 9 : 1;
		
		switch(ytos) {
		case FreshmenFirst: 	return admitTerm == 1 ? stringify(admitYear, 1) : stringify(admitYear, 9);
		case FreshmenSecond: 	return admitTerm == 1 ? stringify(admitYear, 7) : stringify(admitYear+1, 1);
		case SophomoreFirst: 	return admitTerm == 1 ? stringify(admitYear+1, 1) : stringify(admitYear+1, 9);
		case SophomoreSecond:	return admitTerm == 1 ? stringify(admitYear+1, 7) : stringify(admitYear+2, 1);
		case JuniorFirst: 		return admitTerm == 1 ? stringify(admitYear+2, 1) : stringify(admitYear+2, 9);
		case JuniorSecond: 		return admitTerm == 1 ? stringify(admitYear+2, 7) : stringify(admitYear+3, 1);
		case SeniorFirst: 		return admitTerm == 1 ? stringify(admitYear+3, 1) : stringify(admitYear+3, 9);
		case SeniorSecond: 		return admitTerm == 1 ? stringify(admitYear+3, 7) : stringify(admitYear+4, 1);
		default: 				return null;
		}
	}
	
	private static String stringify(int year, int term) {
		return year + "" + term;
	}
	
	@Override
	public String toString() {
		
		return this.asString;
	}
}
