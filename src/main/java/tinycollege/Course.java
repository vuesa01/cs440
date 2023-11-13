package tinycollege;

public class Course {
    private final String CId;
    private final String Title;
    private final String DeptId;

    public Course(String CId, String Title, String DeptId) {
        this.CId = CId;
        this.Title = Title;
        this.DeptId = DeptId;
    }

    /**
     * @return course ID
     */
    public String getCId() {
        return this.CId;
    }

    /**
     * @return course title
     */
    public String getTitle() {
        return this.Title;
    }

    /**
     * @return course department ID
     */
    public String getDeptId() {
        return this.DeptId;
    }
}
