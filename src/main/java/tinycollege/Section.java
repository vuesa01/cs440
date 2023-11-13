package tinycollege;

public class Section {
    private final String SectId;
    private final String CourseId;
    private final String Prof;
    private final String YearOffered;

    public Section(String SectId, String CourseId, String Prof, String YearOffered) {
        this.SectId = SectId;
        this.CourseId = CourseId;
        this.Prof = Prof;
        this.YearOffered = YearOffered;
    }

    /**
     * @return section ID
     */
    public String getSectId() {
        return this.SectId;
    }

    /**
     * @return course ID
     */
    public String getCourseId() {
        return this.CourseId;
    }

    /**
     * @return professor
     */
    public String getProf() {
        return this.Prof;
    }

    /**
     * @return year offered
     */
    public String getYearOffered() {
        return this.YearOffered;
    }

}
