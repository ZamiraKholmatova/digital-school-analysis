-- courses that are not deleted
COPY (
    SELECT id, external_id, system_code, course_name, grades from educational_courses where deleted = 'f'
) TO '/tmp/export_34625/educational_courses_only_courses_16.11.csv' DELIMITER ',' CSV HEADER;

-- all courses
COPY educational_courses(id,deleted,course_type_id,parent_id,external_link,external_id,external_parent_id,course_name,system_code)  TO '/tmp/export_34625/educational_courses_16.11.csv' DELIMITER ',' CSV HEADER;

-- course types (topic, subject, etc.)
COPY educational_course_type(id, type_name)  TO '/tmp/export_34625/educational_course_type_16.11.csv' DELIMITER ',' CSV HEADER;

-- statistics
--COPY educational_course_statistic  TO '/tmp/export_34625/educational_course_statistic_16.11.csv' DELIMITER ',' CSV HEADER;
COPY (
    SELECT
    profile_id, educational_course_id, educational_course_statistic.created_at, entity_id as "statistics_type"
    FROM
    educational_course_statistic
    JOIN educational_courses ON educational_course_id = educational_courses.id
    JOIN statistic_type ON statistic_type.id = statistic_type_id
    WHERE
    (
        system_code = '0b37f22e-c46c-4d53-b0e7-8bdaaf51a8d0'::uuid OR
        system_code = '3a4b37c1-1f7d-4cb9-b144-e24c708d9c20'::uuid
    ) AND
    entity_id = 2 or entity_id = 3
    ORDER BY educational_course_statistic.created_at
) TO '/tmp/export_34625/educational_course_statistic_16.11.csv' DELIMITER ',' CSV HEADER;

-- statistics type description
COPY statistic_type  TO '/tmp/export_34625/statistic_type_16.11.csv' DELIMITER ',' CSV HEADER;
