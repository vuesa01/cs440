package mlb;

/**
 * @author Roman Yasinovskyy
 */
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseWriter {

    public final String SQLITEDBPATH = "jdbc:sqlite:data/mlb/";

    /**
     * @param filename (JSON file)
     * @return League
     */
    public ArrayList<Team> readTeamFromJson(String filename) {
        ArrayList<Team> league = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser;

        try {
            jsonParser = jsonFactory.createParser(new File(filename));
            jsonParser.nextToken();
            while (jsonParser.nextToken() == JsonToken.START_OBJECT) {
                Team team = mapper.readValue(jsonParser, Team.class);
                league.add(team);
            }
            jsonParser.close();
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return league;
    }

    /**
     * @param filename (TXT file)
     * @return Addresses
     */
    public ArrayList<Address> readAddressFromTxt(String filename) {
        ArrayList<Address> addressBook = new ArrayList<>();

        try {
            Scanner fs = new Scanner(new File(filename));
            // TODO: Parse each line into an object of type Address and add it to the ArrayList
            while (fs.hasNextLine()) {
                String[] lineSplit = fs.nextLine().split("\t");
                Address address = new Address(lineSplit[0], lineSplit[1], lineSplit[2], lineSplit[3], lineSplit[4], lineSplit[5], lineSplit[6], lineSplit[7]);
                addressBook.add(address);
            }
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return addressBook;
    }

    public ArrayList<Player> readPlayerFromCsv(String filename) {
        ArrayList<Player> roster = new ArrayList<>();
        CSVReader reader = null;
        // TODO: Read the CSV file, create an object of type Player from each line and add it to the ArrayList
        try {
            reader = new CSVReaderBuilder(new FileReader(filename)).build();
            List<String[]> players = reader.readAll();
            int counter = 0;
            for (String[] aPlayer: players) {
                if (counter != 0) {
                    Player player = new Player(aPlayer[0], aPlayer[1], aPlayer[4], aPlayer[2]);
                    roster.add(player);
                }
                counter += 1;
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (CsvException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return roster;
    }

    /**
     * Create tables cities and teams
     *
     * @param db_filename
     * @throws SQLException
     */
    public void createTables(String db_filename) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        Statement statement = db_connection.createStatement();

        statement.executeUpdate("DROP TABLE IF EXISTS team;");
        statement.executeUpdate("CREATE TABLE team ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "id TEXT NOT NULL,"
                + "abbr TEXT NOT NULL,"
                + "name TEXT NOT NULL,"
                + "conference TEXT NOT NULL,"
                + "division TEXT NOT NULL,"
                + "logo BLOB);");

        statement.execute("PRAGMA foreign_keys = ON;");

        statement.executeUpdate("DROP TABLE IF EXISTS player;");
        statement.executeUpdate("CREATE TABLE player ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "id TEXT NOT NULL,"
                + "name TEXT NOT NULL,"
                + "team TEXT NOT NULL,"
                + "position TEXT NOT NULL,"
                + "FOREIGN KEY (team) REFERENCES team(idpk));");

        statement.executeUpdate("DROP TABLE IF EXISTS address;");
        statement.executeUpdate("CREATE TABLE address ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "team TEXT NOT NULL,"
                + "site TEXT NOT NULL,"
                + "street TEXT NOT NULL,"
                + "city TEXT NOT NULL,"
                + "state TEXT NOT NULL,"
                + "zip TEXT NOT NULL,"
                + "phone TEXT NOT NULL,"
                + "url TEXT NOT NULL,"
                + "FOREIGN KEY (team) REFERENCES team(idpk));");
        db_connection.close();
    }

    /**
     * Read the file and returns the byte array
     *
     * @param filename
     * @return the bytes of the file
     */
    private byte[] readLogoFile(String filename) {
        ByteArrayOutputStream byteArrOutStream = null;
        try {
            File fileIn = new File(filename);
            FileInputStream fileInStream = new FileInputStream(fileIn);
            byte[] buffer = new byte[1024];
            byteArrOutStream = new ByteArrayOutputStream();
            for (int len; (len = fileInStream.read(buffer)) != -1;) {
                byteArrOutStream.write(buffer, 0, len);
            }
            fileInStream.close();
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        } catch (IOException e2) {
            System.err.println(e2.getMessage());
        }
        return byteArrOutStream != null ? byteArrOutStream.toByteArray() : null;
    }

    /**
     * @param db_filename
     * @param league
     * @throws java.sql.SQLException
     */
    public void writeTeamTable(String db_filename, ArrayList<Team> league) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        db_connection.setAutoCommit(false);
        // TODO: Write an SQL statement to insert a new team into a table
        String sql = "INSERT INTO team VALUES (?, ?, ?, ?, ?, ?, ?)";
        // byte[] allLogos = readLogoFile("data/images/mlb");
        for (Team team : league) {
            String logoFileName = "./images/mlb/logo_" + team.getAbbreviation().toLowerCase() + ".jpg";
            byte[] logoData = readLogoFile(logoFileName);
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            // TODO: match parameters of the SQL statement and team id, abbreviation, name, conference, division, and logo
            statement_prepared.setString(2, team.getId());
            statement_prepared.setString(3, team.getAbbreviation());
            statement_prepared.setString(4, team.getName());
            statement_prepared.setString(5, team.getConference());
            statement_prepared.setString(6, team.getDivision());
            statement_prepared.setBytes(7, logoData);
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }

    /**
     * @param db_filename
     * @param addressBook
     * @throws java.sql.SQLException
     */
    public void writeAddressTable(String db_filename, ArrayList<Address> addressBook) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        db_connection.setAutoCommit(false);
        for (Address address : addressBook) {
            // TODO: Write an SQL statement to insert a new address into a table
            Statement statement = db_connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT idpk FROM team WHERE team.name = '" + address.getTeam() + "';");
            int teamId = results.getInt(1);

            String sql = "INSERT INTO address VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            // TODO: match parameters of the SQL statement and address site, street, city, state, zip, phone, and url
            statement_prepared.setInt(2, teamId);
            statement_prepared.setString(3, address.getArena());
            statement_prepared.setString(4, address.getStreet());
            statement_prepared.setString(5, address.getCity());
            statement_prepared.setString(6, address.getState());
            statement_prepared.setString(7, address.getZip());
            statement_prepared.setString(8, address.getPhone());
            statement_prepared.setString(9, address.getUrl());
            statement_prepared.executeUpdate();
                
        }
        db_connection.commit();
        db_connection.close();
    }

    /**
     * @param db_filename
     * @param roster
     * @throws java.sql.SQLException
     */
    public void writePlayerTable(String db_filename, ArrayList<Player> roster) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        db_connection.setAutoCommit(false);
        for (Player player : roster) {
            // TODO: Write an SQL statement to insert a new player into a table
            Statement statement = db_connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT idpk FROM team WHERE team.name = '" + player.getTeam() + "';");
            int teamId = results.getInt(1);
            
            String sql = "INSERT INTO player VALUES (?, ?, ?, ?, ?)";
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            // TODO: match parameters of the SQL statement and player id, name, position
            statement_prepared.setString(2, player.getId());
            statement_prepared.setString(3, player.getName());
            statement_prepared.setInt(4, teamId);
            statement_prepared.setString(5, player.getPosition());
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }
}
