package edu.rutgers.ess.crs.utility;

public enum Grade {

	A(4.0, "A"), B_PLUS(3.5, "B+"), B(3.0, "B"), C_PLUS(2.5, "C+"), C(2.0, "C"), D(1.0, "D"), F(0.0, "F"), PA(1.0, "PA"), NC(0.0, "NC"), NULL(-1.0, "NULL");

	private final double gradePoints;
	private String asString;

	private Grade(double gradePoints, String asString) {

		this.gradePoints = gradePoints;
		this.asString = asString;
	}

	public double getGradePoints() {
		return this.gradePoints;
	}

	public double getGradeWeights() {
		if (this == PA || this == NC) {
			return this.gradePoints / 1.0;
		} else {
			return this.gradePoints / 4.0;
		}
	}

	public static Grade fromString(String text) {
		if (text != null) {
			for (Grade b : Grade.values()) {
				if (text.equalsIgnoreCase(b.asString)) {
					return b;
				}
			}
		}
		if (text == null || text == "" || text == " " || text == "W") {
			throw new IllegalArgumentException("grade not recognized " + text);
		}
		return Grade.F;
	}

	@Override
	public String toString() {
		return this.asString;
	}
}
