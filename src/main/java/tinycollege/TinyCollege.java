package tinycollege;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

public class TinyCollege {
    
    public void initializeCourses() {
        
        Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate();        
        db.getCollection("Courses").drop();
        NitriteCollection collection = db.getCollection("Courses");
        ArrayList<Course> courseList = new ArrayList<>();
        CSVReader reader = null;

        try {
            reader = new CSVReaderBuilder(new FileReader("data/college/COURSE.csv")).build();
            List<String[]> courses = reader.readAll();
            int counter = 0;
            for (String[] aCourse: courses) {
                if (counter != 0) {
                    Course course = new Course(aCourse[0], aCourse[1], aCourse[2]);
                    courseList.add(course);
                }
                counter += 1;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        }

        for (Course course : courseList) {
            Document doc = Document.createDocument("CId", course.getCId())
                .put("Title", course.getTitle())
                .put("DeptId", course.getDeptId());

            collection.insert(doc);
        }

        db.close();

    }

    public void initializeDepartments() {

        Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate(); 
        db.getCollection("Departments").drop();       
        NitriteCollection collection = db.getCollection("Departments");
        ArrayList<Department> deptList = new ArrayList<>();
        CSVReader reader = null;

        try {
            reader = new CSVReaderBuilder(new FileReader("data/college/DEPT.csv")).build();
            List<String[]> departments = reader.readAll();
            int counter = 0;
            for (String[] aDept: departments) {
                if (counter != 0) {
                    Department department = new Department(aDept[0], aDept[1]);
                    deptList.add(department);
                }
                counter += 1;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        }

        for (Department dept : deptList) {
            Document doc = Document.createDocument("DId", dept.getDId())
                .put("DName", dept.getDName());

            collection.insert(doc);
        }

        db.close();

    }

    public void initializeEnrollments() {

        Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate();
        db.getCollection("Enrollments").drop();        
        NitriteCollection collection = db.getCollection("Enrollments");
        ArrayList<Enroll> enrollList = new ArrayList<>();
        CSVReader reader = null;

        try {
            reader = new CSVReaderBuilder(new FileReader("data/college/ENROLL.csv")).build();
            List<String[]> enrollments = reader.readAll();
            int counter = 0;
            for (String[] anEnrollment: enrollments) {
                if (counter != 0) {
                    Enroll enrollment = new Enroll(anEnrollment[0], anEnrollment[1], anEnrollment[2], anEnrollment[3]);
                    enrollList.add(enrollment);
                }
                counter += 1;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        }

        for (Enroll enrollment : enrollList) {
            Document doc = Document.createDocument("EId", enrollment.getEId())
                .put("StudentId", enrollment.getStudentId())
                .put("SectionId", enrollment.getSectionId())
                .put("Grade", enrollment.getGrade());

            collection.insert(doc);
        }

        db.close();

    }

    public void initializeSections() {

        Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate(); 
        db.getCollection("Sections").drop();       
        NitriteCollection collection = db.getCollection("Sections");
        ArrayList<Section> sectionList = new ArrayList<>();
        CSVReader reader = null;

        try {
            reader = new CSVReaderBuilder(new FileReader("data/college/SECTION.csv")).build();
            List<String[]> sections = reader.readAll();
            int counter = 0;
            for (String[] aSection: sections) {
                if (counter != 0) {
                    Section section = new Section(aSection[0], aSection[1], aSection[2], aSection[3]);
                    sectionList.add(section);
                }
                counter += 1;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        }

        for (Section sectoin : sectionList) {
            Document doc = Document.createDocument("SectId", sectoin.getSectId())
                .put("CourseId", sectoin.getCourseId())
                .put("Prof", sectoin.getProf())
                .put("YearOffered", sectoin.getYearOffered());

            collection.insert(doc);
        }

        db.close();

    }

    public void initializeStudents() {

        Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate();        
        db.getCollection("Students").drop();
        NitriteCollection collection = db.getCollection("Students");
        ArrayList<Student> studentList = new ArrayList<>();
        CSVReader reader = null;

        try {
            reader = new CSVReaderBuilder(new FileReader("data/college/STUDENT.csv")).build();
            List<String[]> students = reader.readAll();
            int counter = 0;
            for (String[] aStudent: students) {
                if (counter != 0) {
                    Student student = new Student(aStudent[0], aStudent[1], aStudent[2], aStudent[3]);
                    studentList.add(student);
                }
                counter += 1;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        }

        for (Student student : studentList) {
            Document doc = Document.createDocument("SId", student.getSId())
                .put("SName", student.getSName())
                .put("GradYear", student.getGradYear())
                .put("MajorId", student.getMajorId());

            collection.insert(doc);
        }

        db.close();

    }

    // Returns the entire collection of the specified table
    public void queryCollection(String table) {

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection(table);
            collection.find().forEach(document -> 
                System.out.println(document));
        }
        
    }

    // Returns all students with the specified major
    public void queryStudentMajors(String table, String value) {

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection(table);
            collection.find().forEach(document -> {
                if (document.get("MajorId").equals(value)) {
                    System.out.println(document);
                }
            });
        }
        
    }

    // Returns the record of a course based on title
    public void queryCourseTitle(String value) {

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection("Courses");
            collection.find().forEach(document -> {
                if (document.get("Title").equals(value)) {
                    System.out.println(document);
                }
            });
        }
        
    }

    // Returns the record of a enrollments based on grade
    public void queryEnrollmentGrade(String value) {

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection("Enrollments");
            collection.find().forEach(document -> {
                if (document.get("Grade").equals(value)) {
                    System.out.println(document);
                }
            });
        }
        
    }

    // Returns all student records who graduated before certain year
    public void queryStudentGradYear(int value) {

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection("Students");
            collection.find().forEach(document -> {
                Object gradYearObject = document.get("GradYear");
                String gradYear = (String) gradYearObject;
                int year = Integer.parseInt(gradYear);
                if (year < value) {
                    System.out.println(document);
                }
            });
        }
        
    }    

    // Returns the record of a student based on their enrollment grade
    public void queryEnrollmentGradeForStudent(String value) {

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection("Enrollments");
            NitriteCollection collection2 = db.getCollection("Students");
            collection.find().forEach(document -> {
                collection2.find().forEach(document2 -> {
                    if (document.get("Grade").equals(value) && document.get("StudentId").equals(document2.get("SId"))) {
                        System.out.println(document2);
                    }
                });
            });
        }
        
    }

    // Returns the record of courses that have been taught by a professor in the Sections collection
    public void queryCoursesNotTaught() {

        ArrayList<Document> courseList = new ArrayList<>();

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection("Sections");
            NitriteCollection collection2 = db.getCollection("Courses");
            collection.find().forEach(document -> {
                collection2.find().forEach(document2 -> {
                    if (document.get("CourseId").equals(document2.get("CId")) && !courseList.contains(document2)) {
                        courseList.add(document2);
                        System.out.println(document2);
                    }
                });
            });
        }
        
    }

    // Returns the record of students in the same sections
    public void queryStudentsBySection(String sectionId) {

        ArrayList<Document> enrollList = new ArrayList<>();

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection sections = db.getCollection("Sections");
            NitriteCollection enrollments = db.getCollection("Enrollments");
            NitriteCollection students = db.getCollection("Students");
            sections.find().forEach(sectDoc -> {
                enrollments.find().forEach(enrollDoc -> {
                    if (sectDoc.get("SectId").equals(sectionId) && !enrollList.contains(enrollDoc) && sectDoc.get("SectId").equals(enrollDoc.get("SectionId"))) {
                        enrollList.add(enrollDoc);
                    }
                });
            });

            for (Document doc : enrollList) {
                students.find().forEach(studentDoc -> {
                    if (doc.get("StudentId").equals(studentDoc.get("SId"))) {
                        System.out.println(studentDoc);
                    }
                });
            }
        }
        
    }

    // Returns the professors that have taught by a class in a certain department ID
    public void queryProfsByDepartment(String dept) {

        ArrayList<Document> courseList = new ArrayList<>();

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection collection = db.getCollection("Sections");
            NitriteCollection collection2 = db.getCollection("Courses");
            collection.find().forEach(document -> {
                collection2.find().forEach(document2 -> {
                    if (document.get("CourseId").equals(document2.get("CId")) && !courseList.contains(document2) && document2.get("DeptId").equals(dept)) {
                        courseList.add(document2);
                        System.out.println(document.get("Prof") + " " + document2.get("DeptId"));
                    }
                });
            });
        }
        
    }

    // Returns the student names that have taught by a professor
    public void queryStudentsByProfessor(String professor) {

        ArrayList<Document> sectionList = new ArrayList<>();

        try (Nitrite db = Nitrite.builder().filePath("data/college/lutherDB").openOrCreate()) {
            NitriteCollection sections = db.getCollection("Sections");
            NitriteCollection enrollments = db.getCollection("Enrollments");
            NitriteCollection students = db.getCollection("Students");
            sections.find().forEach(document -> {
                enrollments.find().forEach(document2 -> {
                    if (document.get("Prof").equals(professor) && !sectionList.contains(document2) && document.get("SectId").equals(document2.get("SectionId"))) {
                        sectionList.add(document2);
                    }
                });
            });

            for (Document doc : sectionList) {
                students.find().forEach(studentDoc -> {
                    if (doc.get("StudentId").equals(studentDoc.get("SId"))) {
                        System.out.println(studentDoc.get("SName"));
                    }
                });
            }
        }
        
    }

    public static void main(String[] args) {
        TinyCollege tinyCollege = new TinyCollege(); 

        tinyCollege.initializeCourses();
        tinyCollege.initializeDepartments();
        tinyCollege.initializeEnrollments();
        tinyCollege.initializeSections();
        tinyCollege.initializeStudents();

        System.out.println("==========\nQuery Any Table");
        tinyCollege.queryCollection("Students");

        System.out.println("==========\nQuery by Student Majors");
        tinyCollege.queryStudentMajors("Students", "10");

        System.out.println("==========\nQuery by Course Title");
        tinyCollege.queryCourseTitle("calculus");

        System.out.println("==========\nQuery Enrollments by Grade");
        tinyCollege.queryEnrollmentGrade("A");

        System.out.println("==========\nQuery Students who Graduated Before a Graduation Year");
        tinyCollege.queryStudentGradYear(2021);

        System.out.println("==========\nQuery Students by Grade");
        tinyCollege.queryEnrollmentGradeForStudent("A");
        
        System.out.println("==========\nQuery by Courses not Taught by a Professor in the Sections Collection");
        tinyCollege.queryCoursesNotTaught();

        System.out.println("==========\nQuery Students by Section");
        tinyCollege.queryStudentsBySection("43");

        System.out.println("==========\nQuery for Professors by Department ID");
        tinyCollege.queryProfsByDepartment("10");

        System.out.println("==========\nQuery for Students by Professor");
        tinyCollege.queryStudentsByProfessor("turing");
        }

}

