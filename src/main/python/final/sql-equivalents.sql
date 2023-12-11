-- Query/Select
-- SELECT SId, SName
-- FROM STUDENT;

-- Rename
-- SELECT SId, SName as StudentName, GradYear, MajorId
-- FROM STUDENT;

-- Extend
-- SELECT SId, SName, GradYear, MajorId, 2023-GradYear as YearsAfterGraduating
-- FROM STUDENT;

-- Select
-- SELECT *
-- FROM STUDENT
-- WHERE MajorId = 10;

-- Sort
-- SELECT *
-- FROM STUDENT
-- ORDER BY GradYear, MajorId;

-- GroupBy
-- SELECT GradYear, count(SName)
-- FROM STUDENT
-- GROUP BY GradYear;

-- Product
-- SELECT *
-- FROM STUDENT
-- CROSS JOIN ENROLL;

-- Union
-- SELECT *
-- FROM STUDENT s1
-- UNION
-- SELECT *
-- FROM STUDENT s2

-- Join
-- SELECT *
-- FROM STUDENT s
-- JOIN ENROLL e ON s.SId = e.StudentId;

-- Semi Join
-- SELECT s.SId
-- FROM  STUDENT s
-- WHERE EXISTS (SELECT 1 FROM ENROLL e
--               WHERE e.StudentId = s.SId);

-- Anti Join
-- SELECT *
-- FROM STUDENT s
-- WHERE NOT EXISTS (SELECT 1
--                   FROM ENROLL e
-- 				  WHERE e.StudentId = s.SId);

-- Outer Join
-- SELECT *
-- FROM STUDENT s
-- LEFT OUTER JOIN ENROLL e ON s.SId = e.StudentId;