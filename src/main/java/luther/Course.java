package luther;

public class Course {
    private final String Name;
    private final String Title;
    private final String Professor;
    private final String Credits;
    private final String Day;
    private final String Location;
    private final String Start;
    private final String End;

    public Course(String Name, String Title, String Professor, String Credits, String Day, String Location, String Start, String End) {
        this.Name = Name;
        this.Title = Title;
        this.Professor = Professor;
        this.Credits = Credits;
        this.Day = Day;
        this.Location = Location;
        this.Start = Start;
        this.End = End;
    }

    /**
     * @return Player id
     */
    public String getName() {
        return this.Name;
    }

    /**
     * @return Player name
     */
    public String getTitle() {
        return this.Title;
    }

    /**
     * @return Player team
     */
    public String getProfessor() {
        return this.Professor;
    }

    /**
     * @return Player position
     */
    public String getCredits() {
        return this.Credits;
    }

    public String getDay() {
        return Day;

    }
    public String getLocation() {
        return Location;
    }

    public String getStart() {
        return Start;
    }
    
    public String getEnd() {
        return End;
    }
}
