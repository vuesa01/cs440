package tinycollege;

public class Enroll {
    private final String EId;
    private final String StudentId;
    private final String SectionId;
    private final String Grade;

    public Enroll(String EId, String StudentId, String SectionId, String Grade) {
        this.EId = EId;
        this.StudentId = StudentId;
        this.SectionId = SectionId;
        this.Grade = Grade;
    }

    /**
     * @return enrollment ID
     */
    public String getEId() {
        return this.EId;
    }

    /**
     * @return enrolled student ID
     */
    public String getStudentId() {
        return this.StudentId;
    }

    /**
     * @return enrolled section ID
     */
    public String getSectionId() {
        return this.SectionId;
    }

    /**
     * @return enrollment grade
     */
    public String getGrade() {
        return this.Grade;
    }

}
