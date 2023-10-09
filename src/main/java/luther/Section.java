package luther;

public class Section {
    private final String Name;
    private final String Title;
    private final String Student;

    public Section(String Name, String Title, String Student) {
        this.Name = Name;
        this.Title = Title;
        this.Student = Student;
    }

    public String getName() {
        return this.Name;
    }

    public String getTitle() {
        return this.Title;
    }

    public String getStudent() {
        return this.Student;
    }

}
