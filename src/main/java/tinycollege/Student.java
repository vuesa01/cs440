package tinycollege;

public class Student {
    private final String SId;
    private final String SName;
    private final String GradYear;
    private final String MajorId;

    public Student(String SId, String SName, String GradYear, String MajorId) {
        this.SId = SId;
        this.SName = SName;
        this.GradYear = GradYear;
        this.MajorId = MajorId;
    }

    /**
     * @return student ID
     */
    public String getSId() {
        return this.SId;
    }

    /**
     * @return student name
     */
    public String getSName() {
        return this.SName;
    }

    /**
     * @return graduation year
     */
    public String getGradYear() {
        return this.GradYear;
    }

    /**
     * @return major ID
     */
    public String getMajorId() {
        return this.MajorId;
    }

}
