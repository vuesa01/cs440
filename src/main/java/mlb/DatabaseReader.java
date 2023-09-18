package mlb;

/**
 * @author Roman Yasinovskyy
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseReader {

    private Connection db_connection;
    private final String SQLITEDBPATH = "jdbc:sqlite:data/mlb/mlb.sqlite";

    public DatabaseReader() {
    }

    /**
     * Connect to a database (file)
     */
    public void connect() {
        try {
            this.db_connection = DriverManager.getConnection(SQLITEDBPATH);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReaderGUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Disconnect from a database (file)
     */
    public void disconnect() {
        try {
            this.db_connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReaderGUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Populate the list of divisions
     *
     * @param divisions
     */
    public void getDivisions(ArrayList<String> divisions) {
        Statement stat;
        ResultSet results;

        this.connect();
        try {
            stat = this.db_connection.createStatement();
            // TODO: Write an SQL statement to retrieve a league (conference) and a division
            String sql = "SELECT DISTINCT conference, division FROM team";
            results = stat.executeQuery(sql);
            // TODO: Add all 6 combinations to the ArrayList divisions
            while (results.next()) {
                divisions.add(results.getString("conference") + " | " + results.getString("division"));
            }
            results.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }
    }

    /**
     * Read all teams from the database
     *
     * @param confDiv
     * @param teams
     */
    public void getTeams(String confDiv, ArrayList<String> teams) {
        Statement stat;
        ResultSet results;
        String conference = confDiv.split(" | ")[0];
        String division = confDiv.split(" | ")[2];

        this.connect();
        try {
            stat = this.db_connection.createStatement();
            // TODO: Write an SQL statement to retrieve a teams from a specific division
            String sql = "SELECT name FROM team WHERE conference = '" + conference + "' AND division = '" + division + "'";
            results = stat.executeQuery(sql);
            // TODO: Add all 5 teams to the ArrayList teams
            while (results.next()) {
                teams.add(results.getString("name"));
            }
            results.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }
    }

    /**
     * @param teamName
     * @return Team info
     * @throws SQLException
     */
    public Team getTeamInfo(String teamName) {
        Statement stat;
        ResultSet results;
        Team team = null;
        ArrayList<Player> roster = new ArrayList<>();
        Address address = null;

        // TODO: Retrieve team info (roster, address, and logo) from the database
        this.connect();
        try {
            stat = this.db_connection.createStatement();
    
            String sql = "SELECT * FROM team WHERE name = '" + teamName + "'";
            results = stat.executeQuery(sql);
            team = new Team(results.getString("idpk"), results.getString("abbr"), results.getString("name"), results.getString("conference"), results.getString("division"));
    
            sql = "SELECT * FROM player, team WHERE team.name = '" + teamName + "' AND team.idpk = player.team";
            results = stat.executeQuery(sql);
            while (results.next()) {
                Player player = new Player(results.getString("id"), results.getString("name"), results.getString("team"), results.getString("position"));
                roster.add(player);
                team.setRoster(roster);
            }
            sql = "SELECT * FROM address, team WHERE team.name = '" + teamName + "' AND team.idpk = address.team";
            results = stat.executeQuery(sql);
            while (results.next()) {
                address = new Address(results.getString("team"), results.getString("site"), results.getString("street"), results.getString("city"), results.getString("state"), results.getString("zip"), results.getString("phone"), results.getString("url"));
                team.setAddress(address);
            }
            sql = "SELECT logo FROM team WHERE team.name = '" + teamName + "';";
            results = stat.executeQuery(sql);
            byte[] logo = results.getBytes("logo");
            team.setLogo(logo);
    
            results.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }
        return team;
    }
}
