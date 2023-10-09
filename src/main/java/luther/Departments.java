package luther;

public class Departments {
    private final String DName;
    private final String DHead;
    private final String DHeadEmail;
    private final String DHeadOffice;
    private final String DHeadPhone;

    public Departments(String DName, String DHead, String DHeadEmail, String DHeadOffice, String DHeadPhone) {
        this.DName = DName;
        this.DHead = DHead;
        this.DHeadEmail = DHeadEmail;
        this.DHeadOffice = DHeadOffice;
        this.DHeadPhone = DHeadPhone;
    }

    /**
     * @return Player id
     */
    public String getDName() {
        return this.DName;
    }

    /**
     * @return Player name
     */
    public String getDHead() {
        return this.DHead;
    }

    /**
     * @return Player team
     */
    public String getDHeadEmail() {
        return this.DHeadEmail;
    }

    /**
     * @return Player position
     */
    public String getDHeadOffice() {
        return this.DHeadOffice;
    }

    public String getDHeadPhone() {
        return this.DHeadPhone;
    }
}
