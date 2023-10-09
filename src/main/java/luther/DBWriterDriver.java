package luther;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mysql.cj.x.protobuf.MysqlxDatatypes.Array;

import mlb.DatabaseWriterDriver;

public class DBWriterDriver {
    
    private static final int SectionId = 0;

    public static void main(String[] args) throws SQLException, IOException {
        DBWriter dw = new DBWriter();
        String db_filename = "luther.sqlite";
        int current_year = 2024;
        int section_id = 87;
        if (args.length != 0) {
            db_filename = args[0];
        }
        try {
            dw.createTables(db_filename);
        } catch (SQLException ex) {
            Logger.getLogger(DBWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<String> majors = dw.readMajorsFile("data/luther/programs.txt");
        try {
            dw.writeMajorTable(db_filename, majors);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            dw.writeStudentTable(db_filename);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<Departments> departments = dw.readDepartmentFile("data/luther/departments.txt");
        try {
            dw.writeDepartmentTable(db_filename, departments);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<String> locations = dw.readBuildingFile("data/luther/buildings.txt");
        try {
            dw.writeLocationTable(db_filename, locations);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            dw.writeSemesterTable(db_filename);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<Faculty> facultyList = dw.readFacultyPDF("data/luther/2023_24_Faculty_Directory_2.pdf");
        try {
            dw.writeFacultyTable(db_filename, facultyList);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<Course> course = dw.readCourseHTML("http://www.faculty.luther.edu/~bernatzr/Registrar-Public/Course%20Enrollments/enrollments_FA2023.htm");
        try {
            dw.writeCourseTable(db_filename, course);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<Section> section = dw.readSectionFromTxt("data/luther/sections.txt");
        try {
            dw.writeSectionTable(db_filename, section);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        ArrayList<Enrollment> enrollments = dw.readEnrollmentFromTxt("data/luther/enrollment.txt");
        try {
            dw.writeEnrollmentsTable(db_filename, enrollments);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            ArrayList<String> seniors = dw.findSeniors(db_filename, current_year);
            System.out.println("\nSenior List:");
            for (String name : seniors) {
                System.out.println(name); // Tests to see if the view works, it does
            }
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            ArrayList<String> students = dw.findAllStudentsInASection(db_filename, section_id);
            System.out.println("\nStudents Section List:");
            for (String name : students) {
                System.out.println(name); // Tests to see if the view works, it does
            }
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseWriterDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
