COPY profile_role  TO '/tmp/profile_role_16.11.csv' DELIMITER ',' CSV HEADER;
COPY role(id, role)  TO '/tmp/role_16.11.csv' DELIMITER ',' CSV HEADER;
COPY profile_educational_institution  TO '/tmp/profile_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;
COPY external_system(id, short_name, system_code)  TO '/tmp/external_system_16.11.csv' DELIMITER ',' CSV HEADER;
COPY student(id, grade) TO '/tmp/student_16.11.csv' DELIMITER ',' CSV HEADER;
-- COPY (
--     SELECT student_id, grade_educational_institution_id FROM student_grade_educational_institution WHERE is_actual = 't'
-- ) TO '/tmp/student_grade_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;
-- COPY grade_educational_institution(id, grade) TO '/tmp/grade_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;

\c stat_service;
COPY (
    SELECT id, external_id, system_code, course_name, grades from educational_courses where deleted = 'f'
) TO '/tmp/educational_courses_only_courses_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_courses(id,deleted,course_type_id,parent_id,external_link,external_id,external_parent_id,course_name,system_code)  TO '/tmp/educational_courses_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_course_type(id, type_name)  TO '/tmp/educational_course_type_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_course_statistic  TO '/tmp/educational_course_statistic_16.11.csv' DELIMITER ',' CSV HEADER;
COPY statistic_type  TO '/tmp/statistic_type_16.11.csv' DELIMITER ',' CSV HEADER;
