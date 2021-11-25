COPY (
    SELECT id, external_id, system_code, course_name, grades from educational_courses where deleted = 'f'
) TO '/tmp/export_34625/educational_courses_only_courses_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_courses(id,deleted,course_type_id,parent_id,external_link,external_id,external_parent_id,course_name,system_code)  TO '/tmp/export_34625/educational_courses_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_course_type(id, type_name)  TO '/tmp/export_34625/educational_course_type_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_course_statistic  TO '/tmp/export_34625/educational_course_statistic_16.11.csv' DELIMITER ',' CSV HEADER;
COPY statistic_type  TO '/tmp/export_34625/statistic_type_16.11.csv' DELIMITER ',' CSV HEADER;
