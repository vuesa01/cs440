package luther;

public class Faculty {
    private final String Name;
    private final String Office;
    private final String Extension;
    private final String Username;

    public Faculty(String Name, String Office, String Extension, String Username) {
        this.Name = Name;
        this.Office = Office;
        this.Extension = Extension;
        this.Username = Username;
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
    public String getOffice() {
        return this.Office;
    }

    /**
     * @return Player team
     */
    public String getExtension() {
        return this.Extension;
    }

    /**
     * @return Player position
     */
    public String getUsername() {
        return this.Username;
    }
}
