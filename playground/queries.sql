copy (
select 
educational_courses.id, educational_courses.external_id, short_name, course_name,
jsonb_array_elements(educational_courses.grades)->>0::int as "grade"
from educational_courses 
join educational_course_type on course_type_id = educational_course_type.id 
join external_system on educational_courses.system_code = uuid(external_system.system_code)
where type_name = 'ЦОМ' and deleted = 'f'
) to '/tmp/courses_and_platforms.csv' DELIMITER ',' CSV HEADER;

несколько записей
select id, external_id, system_code, course_name, grades, deleted, approved from educational_courses where course_name = 'Интерактивный курс Учи.ру. Математика 1 класс.' and deleted = 'f';

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
        SELECT DISTINCT smartcode_id FROM smartcode_external_system
    ) AS activated 
    LEFT JOIN smartcode ON activated.smartcode_id = smartcode.id 
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
    GROUP BY eins.region, eins.short_name, eins.inn
) TO '/tmp/export_34625/Подтверждено_по_школам.csv' DELIMITER ',' CSV HEADER;


SELECT
date AS "Дата", 
COUNT(CASE WHEN role = 'STUDENT' THEN 1 ELSE NULL END) AS "Студентов",
COUNT(CASE WHEN role = 'TEACHER' THEN 1 ELSE NULL END) AS "Преподавателей",
FROM (
    SELECT 
    DATE(smartcode.created_at) AS "date", ed_inst.profile_id, ed_inst.role
    FROM (
        SELECT DISTINCT smartcode_id FROM smartcode_external_system
    ) AS activated 
    LEFT JOIN smartcode ON activated.smartcode_id = smartcode.id 
    LEFT JOIN profile_educational_institution AS ed_inst ON ed_inst.profile_id = smartcode.profile_id
    WHERE role = 'STUDENT' or role = 'TEACHER'
) as activation_dates GROUP BY date ORDER BY date
LIMIT 10;


SELECT
* 
FROM (
    SELECT 
    eins.region AS "Регион",
    COUNT(CASE WHEN pei.role = 'STUDENT' THEN 1 ELSE NULL END) AS "Студентов",
    COUNT(CASE WHEN pei.role = 'TEACHER' THEN 1 ELSE NULL END) AS "Преподавателей",
    COUNT(DISTINCT short_name) AS "Учреждений"
    FROM (
        SELECT DISTINCT smartcode_id FROM smartcode_external_system
    ) AS activated 
    LEFT JOIN smartcode ON activated.smartcode_id = smartcode.id 
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
    GROUP BY eins.region ORDER BY eins.region
) AS sm_count WHERE "Регион" = 'край Алтайский'
LIMIT 10;


SELECT 
external_system.short_name,
COUNT(1) AS "Всего",
COUNT(CASE WHEN role = 'STUDENT' THEN 1 ELSE NULL END) AS "Студентов",
COUNT(CASE WHEN role = 'TEACHER' THEN 1 ELSE NULL END) AS "Преподавателей"
FROM 
smartcode
JOIN smartcode_external_system ON smartcode.id = smartcode_external_system.smartcode_id
JOIN profile_educational_institution ON smartcode.profile_id = profile_educational_institution.profile_id
JOIN external_system ON smartcode_external_system.system_code = external_system.system_code
GROUP BY external_system.short_name
LIMIT 1;


