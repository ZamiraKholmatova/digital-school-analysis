COPY profile_role  TO '/tmp/export_34625/profile_role_16.11.csv' DELIMITER ',' CSV HEADER;
COPY role(id, role)  TO '/tmp/export_34625/role_16.11.csv' DELIMITER ',' CSV HEADER;
--COPY profile_educational_institution  TO '/tmp/export_34625/profile_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;
COPY (
    SELECT
    profile_id, educational_institution_id, approved_status, role, profile_educational_institution.updated_at,
    profile_educational_confirmation_log.updated_at as "approval_date"
    FROM
    profile_educational_institution
    LEFT JOIN profile_educational_confirmation_log on (profile_educational_institution.id = profile_educational_confirmation_log.profile_educational_institution_id)
) TO '/tmp/export_34625/profile_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;
COPY educational_institution  TO '/tmp/export_34625/educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;
COPY external_system(id, short_name, system_code)  TO '/tmp/export_34625/external_system_16.11.csv' DELIMITER ',' CSV HEADER;
--COPY student(id, grade) TO '/tmp/export_34625/student_16.11.csv' DELIMITER ',' CSV HEADER;
COPY (
    SELECT
    student.id,
    educational_institution_id,
    grade
    FROM
    student
    LEFT JOIN student_grade_educational_institution on (student.id = student_grade_educational_institution.student_id)
    LEFT JOIN grade_educational_institution on (student_grade_educational_institution.grade_educational_institution_id = grade_educational_institution.id)
) TO '/tmp/export_34625/student_16.11.csv' DELIMITER ',' CSV HEADER;

COPY (
    SELECT
    eins.region AS "Регион",
    eins.short_name AS "Школа",
    eins.inn AS "ИНН",
    eins.address AS "Адрес",
    pei.profile_id as "profile_id",
    pei.approved_status as "approved_status",
    pei.role as "role"
    FROM (
        SELECT DISTINCT smartcode_id FROM smartcode_external_system
    ) AS activated
    LEFT JOIN smartcode ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
) TO '/tmp/export_34625/school_students.csv' DELIMITER ',' CSV HEADER;

COPY (
    SELECT
    eins.region AS "Регион",
    eins.short_name AS "Школа",
    eins.inn AS "ИНН",
    COUNT(CASE WHEN pei.role = 'STUDENT' AND pei.approved_status = 'NONE' THEN 1 ELSE NULL END) AS "Не подтвержденные ученики",
    COUNT(CASE WHEN pei.role = 'STUDENT' AND pei.approved_status = 'NOT_APPROVED' THEN 1 ELSE NULL END) AS "Отклоненные ученики",
    COUNT(CASE WHEN pei.role = 'STUDENT' AND pei.approved_status = 'APPROVED' THEN 1 ELSE NULL END) AS "Подтвержденные ученики",
    COUNT(CASE WHEN pei.role = 'TEACHER' AND pei.approved_status = 'NONE' THEN 1 ELSE NULL END) AS "Не подтвержденные преподаватели",
    COUNT(CASE WHEN pei.role = 'TEACHER' AND pei.approved_status = 'NOT_APPROVED' THEN 1 ELSE NULL END) AS "Отклоненные преподаватели",
    COUNT(CASE WHEN pei.role = 'TEACHER' AND pei.approved_status = 'APPROVED' THEN 1 ELSE NULL END) AS "Подтвержденные преподаватели"
    FROM (
        SELECT DISTINCT smartcode_id FROM (
            SELECT smartcode_id FROM smartcode_external_system WHERE is_deleted = false
        ) AS not_deleted
    ) AS activated
    LEFT JOIN smartcode ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
    WHERE smartcode.is_deleted = false AND pei.is_deleted = false
    GROUP BY eins.region, eins.short_name, eins.inn
) TO '/tmp/export_34625/Подтверждено_по_школам.csv' DELIMITER ',' CSV HEADER;



COPY (
    SELECT
    pei.approved_status AS "Статус",
    COUNT(1) AS "Всего активировано",
    COUNT(CASE WHEN pei.role='STUDENT' THEN 1 ELSE NULL END) AS "Студентов", COUNT(CASE WHEN pei.role='TEACHER' THEN 1 ELSE NULL END) AS "Преподавателей" FROM
    (SELECT smartcode_id
    FROM smartcode_external_system GROUP BY smartcode_id)
    AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    WHERE pei.role = 'STUDENT' OR pei.role='TEACHER' GROUP BY approved_status
) TO '/tmp/export_34625/Учеников_и_Преподавателей_по_статусам_подтверждения.csv' DELIMITER ',' CSV HEADER;


COPY (
    SELECT
    external_system.short_name AS platform_name,
    COUNT(1) AS "Активировано",
    COUNT(CASE WHEN pei.approved_status='APPROVED' THEN 1 ELSE NULL END) AS "Подтверждено" FROM smartcode_external_system AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei
    ON pei.profile_id = smartcode.profile_id
    LEFT JOIN external_system
    ON activated.system_code = external_system.system_code
    WHERE pei.role = 'STUDENT' OR pei.role='TEACHER'
    GROUP BY platform_name
) TO '/tmp/export_34625/Активировано_по_платформам.csv' DELIMITER ',' CSV HEADER;


COPY (
    SELECT
    DATE(activated.created_at) AS date_activated,
    COUNT(CASE WHEN pei.role='STUDENT' THEN 1 ELSE NULL END) AS "Студентов", COUNT(CASE WHEN pei.role='TEACHER' THEN 1 ELSE NULL END) AS "Преподавателей" FROM
    (SELECT smartcode_id, MIN(created_at) AS created_at FROM smartcode_external_system
    GROUP BY smartcode_id)
    AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    GROUP BY date_activated
    ORDER BY date_activated
) TO '/tmp/export_34625/Прирост_учеников_и_учителей.csv' DELIMITER ',' CSV HEADER;
