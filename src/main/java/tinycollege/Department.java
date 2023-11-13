package tinycollege;

public class Department {
    private final String DId;
    private final String DName;

    public Department(String DId, String DName) {
        this.DId = DId;
        this.DName = DName;
    }

    /**
     * @return department ID
     */
    public String getDId() {
        return this.DId;
    }

    /**
     * @return department name
     */
    public String getDName() {
        return this.DName;
    }
    
}
