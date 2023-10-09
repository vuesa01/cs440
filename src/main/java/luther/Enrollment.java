package luther;

public class Enrollment {
    private final String Student;
    private final String Section;
    private final String Grade;

    public Enrollment(String Student, String Section, String Grade) {
        this.Student = Student;
        this.Section = Section;
        this.Grade = Grade;
    }

    public String getStudent() {
        return this.Student;
    }

    public String getSection() {
        return this.Section;
    }

    public String getGrade() {
        return this.Grade;
    }
}
