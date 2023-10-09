package luther;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.Buffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.html.parser.Element;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.github.javafaker.Faker;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Array;

import mlb.DatabaseReader;
import netscape.javascript.JSException;

public class DBWriter {

    public final String SQLITEDBPATH = "jdbc:sqlite:data/luther/";

    public void createTables(String db_filename) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        Statement statement = db_connection.createStatement();

        statement.executeUpdate("DROP TABLE IF EXISTS major;");
        statement.executeUpdate("DROP TABLE IF EXISTS section;");
        statement.executeUpdate("DROP TABLE IF EXISTS course;");
        statement.executeUpdate("DROP TABLE IF EXISTS enrollment;");
        statement.executeUpdate("DROP TABLE IF EXISTS student;");
        statement.executeUpdate("DROP TABLE IF EXISTS department;");
        statement.executeUpdate("DROP TABLE IF EXISTS faculty;");
        statement.executeUpdate("DROP TABLE IF EXISTS location;");
        statement.executeUpdate("DROP TABLE IF EXISTS semester;");

        statement.executeUpdate("CREATE TABLE major ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "MajorName TEXT NOT NULL);");
    
        statement.execute("PRAGMA foreign_keys = ON;");

        statement.executeUpdate("CREATE TABLE student ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "Name TEXT,"
                + "GradYear INTEGER," 
                + "Major INTEGER,"
                + "FOREIGN KEY (Major) REFERENCES major(idpk));");
                
                statement.executeUpdate("CREATE TABLE department ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "DName TEXT NOT NULL,"
                + "DHead TEXT,"
                + "DHeadEmail TEXT,"
                + "DHeadOffice TEXT,"
                + "DHeadPhone TEXT);");

        statement.executeUpdate("CREATE TABLE faculty ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "Name TEXT NOT NULL,"
                + "Office TEXT NOT NULL,"
                + "Extension TEXT NOT NULL,"
                + "Username TEXT NOT NULL);");

        statement.executeUpdate("CREATE TABLE location ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "LocationName TEXT NOT NULL,"
                + "RoomNumber INTEGER NOT NULL);");
        
        statement.executeUpdate("CREATE TABLE semester ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "SemesterName TEXT NOT NULL,"
                + "StartDate TEXT NOT NULL,"
                + "EndDate TEXT NOT NULL);");
        
        statement.executeUpdate("CREATE TABLE course ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "Section TEXT NOT NULL,"
                + "Title TEXT NOT NULL,"
                + "Professor TEXT NOT NULL,"
                + "Credits TEXT NOT NULL,"
                + "Days TEXT,"
                + "StartTime TEXT,"
                + "EndTime TEXT,"
                + "Location Text NOT NULL);");        
            
        statement.executeUpdate("CREATE TABLE section ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "CourseNameId INTEGER NOT NULL,"
                + "Title TEXT NOT NULL,"
                + "Student TEXT NOT NULL,"
                + "FOREIGN KEY (CourseNameId) REFERENCES course(idpk),"
                + "FOREIGN KEY (Student) REFERENCES student(Name));");
                
        statement.executeUpdate("CREATE TABLE enrollment ("
                + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                + "Student TEXT,"
                + "SectionId INTEGER,"
                + "Grade TEXT,"
                + "FOREIGN KEY (Student) REFERENCES student(Name),"
                + "FOREIGN KEY (SectionId) REFERENCES section(idpk));"); 
        db_connection.close();
    }

    public ArrayList<String> readMajorsFile(String filename) {
        ArrayList<String> majors = new ArrayList<>();
        
        try {
            Scanner fs = new Scanner(new File(filename));
            while (fs.hasNextLine()) {
                majors.add(fs.nextLine());
            }
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }        
        return majors;
    } 

    public void writeMajorTable(String db_filename, ArrayList<String> majors) throws SQLException{
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        db_connection.setAutoCommit(false);

        String sql = "INSERT INTO major VALUES (?, ?)";

        for (String major : majors) {
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, major);
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }
        
    public void writeStudentTable(String db_filename) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        db_connection.setAutoCommit(false);

        String sql = "INSERT INTO student VALUES (?, ?, ?, ?)";
        Faker faker = new Faker();

        for (int i = 0; i < 15; i++) {
            String name = faker.name().fullName();
            int gradYear = faker.number().numberBetween(2024, 2028);
            int majorId = faker.number().numberBetween(1, 55);

            Statement statement = db_connection.createStatement();
            ResultSet result = statement.executeQuery("SELECT idpk FROM major WHERE major.idpk = '" + majorId + "';");
            majorId = result.getInt(1);

            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, name);
            statement_prepared.setInt(3, gradYear);
            statement_prepared.setInt(4, majorId);
            statement_prepared.executeUpdate();
        }

        // Statement triggerStatement = db_connection.createStatement();
        // triggerStatement.executeUpdate("CREATE TRIGGER set_null_major "
        //         + "AFTER INSERT ON student BEGIN "
        //         + "UPDATE student SET Major = NULL WHERE rowid = NEW.rowid; "
        //         + "END;");
        // String insertSql = "INSERT INTO student VALUES (?, ?, ?, ?)";
        // PreparedStatement statement_insert = db_connection.prepareStatement(insertSql);
        // statement_insert.setString(2, faker.name().fullName());
        // statement_insert.setInt(3, faker.number().numberBetween(2023, 2027));
        // statement_insert.setInt(4, faker.number().numberBetween(1, 55));
        // statement_insert.executeUpdate();

        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<Departments> readDepartmentFile(String db_filename) throws SQLException, IOException {
        String DName = null;
        String DHead = null;
        String DHeadEmail = null;
        String DHeadOffice = null;
        String DHeadPhone = null;
        ArrayList<Departments> departmentData = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(db_filename))) {
            reader.readLine();
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    if (DName != null) {
                        Departments department = new Departments(DName, DHead, DHeadEmail, DHeadOffice, DHeadPhone);
                        departmentData.add(department);
                        DName = DHead = DHeadEmail = DHeadOffice = DHeadPhone = null;
                    }
                }
                else {
                    if (DName == null) {
                        DName = line.trim();
                    }
                    else if (DHead == null) {
                        String[] lineSplit = line.trim().split(",");
                        DHead = lineSplit[0];
                    }
                    else if (DHeadEmail == null) {
                        DHeadEmail = line.trim();
                    }
                    else if (DHeadOffice == null) {
                        DHeadOffice = line.trim();
                    }
                    else if (DHeadPhone == null) {
                        DHeadPhone = line.trim();
                    }
                }
            }
            if (DName != null) {
                Departments department = new Departments(DName, DHead, DHeadEmail, DHeadOffice, DHeadPhone);
                departmentData.add(department);
            }
        }
        return departmentData;
    }

    public void writeDepartmentTable(String db_filename, ArrayList<Departments> department) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);        
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        
        for (Departments aDepartment : department) {
            String sql = "INSERT INTO department VALUES (?, ?, ?, ?, ?, ?)";

            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, aDepartment.getDName());   
            statement_prepared.setString(3, aDepartment.getDHead());
            statement_prepared.setString(4, aDepartment.getDHeadEmail());
            statement_prepared.setString(5, aDepartment.getDHeadOffice());
            statement_prepared.setString(6, aDepartment.getDHeadPhone());    
            statement_prepared.executeUpdate();     
        }
        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<String> readBuildingFile(String filename) {
        ArrayList<String> location = new ArrayList<>();
        
        try {
            Scanner fs = new Scanner(new File(filename));
            while (fs.hasNextLine()) {
                location.add(fs.nextLine());
            }
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }        
        return location;
    } 

    public void writeLocationTable(String db_filename, ArrayList<String> location) throws SQLException{
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);

        String sql = "INSERT INTO location VALUES (?, ?, ?)";

        for (String aLocation : location) {
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, aLocation);
            statement_prepared.setInt(3, 1);
            statement_prepared.executeUpdate();
            statement_prepared.setString(2, aLocation);
            statement_prepared.setInt(3, 2);
            statement_prepared.executeUpdate();
            statement_prepared.setString(2, aLocation);
            statement_prepared.setInt(3, 3);
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }

     public void writeSemesterTable(String db_filename) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);

        String sql = "INSERT INTO semester VALUES (?, ?, ?, ?)";

        PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(2, "Fall 2023");
        statement_prepared.setString(3, "August 30, 2023");
        statement_prepared.setString(4, "December 14, 2023");
        statement_prepared.executeUpdate();

        statement_prepared.setString(2, "JTerm 2024");
        statement_prepared.setString(3, "January 3, 2024");
        statement_prepared.setString(4, "January 26, 2024");
        statement_prepared.executeUpdate();

        statement_prepared.setString(2, "Spring 2024");
        statement_prepared.setString(3, "February 1, 2024");
        statement_prepared.setString(4, "May 16, 2024");
        statement_prepared.executeUpdate();
        
        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<Faculty> readFacultyPDF(String url) throws IOException {
        ArrayList<Faculty> facultyList = new ArrayList<Faculty>();
        String line;
        File thisURL = new File(url);
        PDDocument doc = PDDocument.load(thisURL);
        PDFTextStripper stripper = new PDFTextStripper();
        String text = stripper.getText(doc);

        BufferedReader br = new BufferedReader(new StringReader(text));

        while ((line = br.readLine()) != null) {
            line = line.replaceAll("[^a-zA-Z0-9 ]", "");
            line = line.replaceAll("X ", "");
            line = line.replaceAll("head ", "");
            line = line.replaceAll("acting ", "");
            line = line.replaceAll("headspring ", "");
            line = line.replaceAll("headfall ", "");
            line = line.replaceAll("program ", "");
            line = line.replaceAll("cohead ", "");
            line = line.replaceAll("Maltaspring ", "");
            line = line.replaceAll("fall ", "");
            line = line.replaceAll("co ", "");
            line = line.replaceAll("Mnsterspring ", "");
            line = line.replaceAll("director ", "");
            String[] data = line.trim().split(" ");    
            if (data.length >= 6 && !data[0].equals("Fall") && !data[0].equals("new") && !data[0].equals("parttime")) {
                if (data.length == 6) {
                    Faculty faculty = new Faculty(data[1] + " " + data[0], data[2] + " " + data[3], data[4], data[5]);
                    facultyList.add(faculty);
                }
                else if (data.length == 7) {
                    if (data[0].equals("1") || data[0].equals("2")) {
                        Faculty faculty = new Faculty(data[2] + " " + data[1], data[3] + " " + data[4], data[5], data[6]);
                        facultyList.add(faculty);
                    }
                    else if (data[0].equals("Alonso")) {
                        Faculty faculty = new Faculty(data[2] + " " + data[0] + " " + data[1], data[3] + " " + data[4], data[5], data[6]);
                        facultyList.add(faculty);
                    }
                    else {
                        Faculty faculty = new Faculty(data[1] + " " + data[0], data[2] + " " + data[3] + " " + data[4], data[5], data[6]);
                        facultyList.add(faculty);
                    }
                }
            }
        }
        return facultyList;
    }

    public void writeFacultyTable(String db_filename, ArrayList<Faculty> faculty) throws SQLException{
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);

        String sql = "INSERT INTO faculty VALUES (?, ?, ?, ?, ?)";

        for (Faculty aPerson : faculty) {
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, aPerson.getName());
            statement_prepared.setString(3, aPerson.getOffice());
            statement_prepared.setString(4, aPerson.getExtension());
            statement_prepared.setString(5, aPerson.getUsername());
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<Course> readCourseHTML(String url) throws IOException {
        ArrayList<Course> sectionList = new ArrayList<>();
        Document doc = Jsoup.connect(url).get();
        Elements tables = doc.select("table");
        int count = 0;
        String aSection = null;
        String title = null;
        String profs = null;
        String credits = null;
        String days = null;
        String start = null;
        String end = null;
        String location = null;

        for (org.jsoup.nodes.Element table : tables) {
            Elements rows = table.select("tr");
            for (org.jsoup.nodes.Element row : rows) {
                Elements values = row.select("td");
                for (org.jsoup.nodes.Element value: values) {
                    if (count >= 6) {
                        if (aSection == null) {
                            aSection = value.text();
                        }
                        else if (title == null) {
                            title = value.text();
                        }
                        else if (profs == null) {
                            profs = value.text();
                        }
                        else if (credits == null) {
                            credits = value.text();
                        }
                        else if (days == null && count == 11) {
                            days = value.text();
                        }
                        else if (start == null && count == 12) {
                            start = value.text();
                        }
                        else if (end == null && count == 13) {
                            end = value.text();
                        }
                        else if (location == null && count == 20) {
                            location = value.text();
                            Course section = new Course(aSection, title, profs, credits, days, start, end, location);
                            sectionList.add(section);
                            aSection = title = profs = credits = days = start = end = location = null;
                            count = 4;
                        }
                    }
                    count++;
                }
            }
        }
        return sectionList;
    }

    public void writeCourseTable(String db_filename, ArrayList<Course> course) throws SQLException{
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);

        String sql = "INSERT INTO course VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        for (Course aCourse : course) {
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, aCourse.getName());
            statement_prepared.setString(3, aCourse.getTitle());
            statement_prepared.setString(4, aCourse.getProfessor());
            statement_prepared.setString(5, aCourse.getCredits());
            statement_prepared.setString(6, aCourse.getDay());
            statement_prepared.setString(7, aCourse.getLocation());
            statement_prepared.setString(8, aCourse.getStart());
            statement_prepared.setString(9, aCourse.getEnd());
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<Section> readSectionFromTxt(String filename) {
        ArrayList<Section> sections = new ArrayList<>();

        try {
            Scanner fs = new Scanner(new File(filename));
            while (fs.hasNextLine()) {
                String[] lineSplit = fs.nextLine().split(",");
                Section entrySection = new Section(lineSplit[0], lineSplit[1], lineSplit[2]);
                sections.add(entrySection);
            }
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return sections;
    }

    public void writeSectionTable(String db_filename, ArrayList<Section> sections) throws SQLException{
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);        
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");

        String sql = "INSERT INTO section VALUES (?, ?, ?, ?)";
        
        for (Section aSection : sections) {
            Statement statement = db_connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT idpk FROM course WHERE course.Section = '" + aSection.getName() + "';");
            int sectionId = results.getInt(1);
            results = statement.executeQuery("SELECT Name FROM student WHERE student.idpk = '" + Integer.parseInt(aSection.getStudent()) + "';");
            String studentName = results.getString(1);

            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setInt(2, sectionId);
            statement_prepared.setString(3, aSection.getTitle());
            statement_prepared.setString(4, studentName);
            statement_prepared.executeUpdate();
        }
        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<Enrollment> readEnrollmentFromTxt(String filename) {
        ArrayList<Enrollment> enrollments = new ArrayList<>();

        try {
            Scanner fs = new Scanner(new File(filename));
            while (fs.hasNextLine()) {
                String[] lineSplit = fs.nextLine().split(",");
                Enrollment entrySection = new Enrollment(lineSplit[0], lineSplit[1], lineSplit[2]);
                enrollments.add(entrySection);
            }
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return enrollments;
    }

    public void writeEnrollmentsTable(String db_filename, ArrayList<Enrollment> enrollments) throws SQLException{
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);        
        db_connection.createStatement().execute("PRAGMA foreign_keys = ON;");
        Statement triggerStatement = db_connection.createStatement();

        String sql = "INSERT INTO enrollment VALUES (?, ?, ?, ?)";
        
        for (Enrollment entry : enrollments) {
            Statement statement = db_connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT idpk FROM course WHERE course.Section = '" + entry.getSection() + "';");
            int sectionId = results.getInt(1);
            results = statement.executeQuery("SELECT Name FROM student WHERE student.idpk = '" + Integer.parseInt(entry.getStudent()) + "';");
            String studentName = results.getString(1);

            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            statement_prepared.setString(2, studentName);
            statement_prepared.setInt(3, sectionId);
            statement_prepared.setString(4, entry.getGrade());
            statement_prepared.executeUpdate();
        }

        triggerStatement.executeUpdate("CREATE TRIGGER set_null_grade "
                + "AFTER INSERT ON enrollment BEGIN "
                + "UPDATE enrollment SET Grade = NULL WHERE rowid = NEW.rowid; "
                + "END;");
        String insertSql = "INSERT INTO enrollment VALUES (?, ?, ?, ?)";
        PreparedStatement statement_insert = db_connection.prepareStatement(insertSql);
        ResultSet results = triggerStatement.executeQuery("SELECT Name FROM student WHERE student.idpk = '" + 4 + "';");
        String name = results.getString(1);
        statement_insert.setString(2, name);
        statement_insert.setInt(3, 100);
        statement_insert.setString(4, "B");
        statement_insert.executeUpdate();

        db_connection.commit();
        db_connection.close();
    }

    public ArrayList<String> findSeniors(String db_filename, int gradYear) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        db_connection.setAutoCommit(false);  
        ArrayList<String> allSeniors = new ArrayList<>();

        String viewSql = "CREATE VIEW IF NOT EXISTS seniors AS "
                + "SELECT * FROM student WHERE GradYear = '" + gradYear + "';";
        PreparedStatement preparedStatement = db_connection.prepareStatement(viewSql);
        preparedStatement.execute();

        String seniorSql = "SELECT * FROM seniors";
        Statement statement = db_connection.createStatement();
        ResultSet results = statement.executeQuery(seniorSql);

        while (results.next()) {
            allSeniors.add(results.getString("Name"));
        }
        return allSeniors;
    }

    
}
