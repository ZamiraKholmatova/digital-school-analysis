-- this table contains all users and should be used as a reference
-- flag is deleted should be checked
--COPY (
--    SELECT id as "profile_id", is_deleted FROM profile
--) TO '/tmp/export_34625/profiles_16.11.csv' DELIMITER ',' CSV HEADER;

-- profile role has more records than profile, is this normal?
-- flag is deleted should be checked
COPY profile_role  TO '/tmp/export_34625/profile_role_16.11.csv' DELIMITER ',' CSV HEADER;

-- role contains role descriptions
-- tables profile_role, role are no longer needed
-- profile_educational_institution contains all necessary information
COPY role(id, role)  TO '/tmp/export_34625/role_16.11.csv' DELIMITER ',' CSV HEADER;
--COPY profile_educational_institution  TO '/tmp/export_34625/profile_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;

-- this table contains information about role, approved status and institution, as well as is_deleted flag
-- COPY (
--     SELECT
--     profile_id, educational_institution_id, approved_status, role, profile_educational_institution.updated_at,
--     max(profile_educational_confirmation_log.updated_at) as "approval_date", profile_educational_institution.is_deleted
--     FROM
--     profile_educational_institution
--     LEFT JOIN profile_educational_confirmation_log on (profile_educational_institution.id = profile_educational_confirmation_log.profile_educational_institution_id)
--     WHERE profile_educational_institution.updated_at in (select max(updated_at) from profile_educational_institution group by profile_id)
--     GROUP BY profile_id, profile_educational_institution.educational_institution_id, approved_status, role, profile_educational_institution.updated_at, profile_educational_institution.is_deleted
-- ) TO '/tmp/export_34625/profile_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;

-- UPD: this table contains information about role, approved status and institution, as well as is_deleted flag
COPY (
    SELECT
    pei.profile_id, pei.educational_institution_id, pei.approved_status, pei.role, pei.updated_at,
    max(profile_educational_confirmation_log.updated_at) as "approval_date", pei.is_deleted
    FROM
    profile_educational_institution as pei
    LEFT JOIN profile_educational_confirmation_log on (pei.id = profile_educational_confirmation_log.profile_educational_institution_id)
    WHERE pei.updated_at = (SELECT max(updated_at) from profile_educational_institution as pei1 where pei.profile_id=pei1.profile_id)
    GROUP BY pei.profile_id, pei.educational_institution_id, pei.approved_status, pei.role, pei.updated_at, pei.is_deleted
) TO '/tmp/export_34625/profile_educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;

-- educational_institution contains school address and inn
COPY educational_institution  TO '/tmp/export_34625/educational_institution_16.11.csv' DELIMITER ',' CSV HEADER;

-- external_system stores ids of platforms
COPY external_system(id, short_name, system_code)  TO '/tmp/export_34625/external_system_16.11.csv' DELIMITER ',' CSV HEADER;
--COPY student(id, grade) TO '/tmp/export_34625/student_16.11.csv' DELIMITER ',' CSV HEADER;

-- this table is used solely for grades
-- COPY (
--     SELECT
--     student.id,
--     educational_institution_id,
--     grade,
--     student.is_deleted OR student_grade_educational_institution.is_deleted as "is_deleted"
--     FROM
--     student
-- --     (SELECT * FROM student WHERE created_at in (select max(created_at) from student_grade_educational_institution group by student_id))
--     LEFT JOIN student_grade_educational_institution on (student.id = student_grade_educational_institution.student_id)
--     LEFT JOIN grade_educational_institution on (student_grade_educational_institution.grade_educational_institution_id = grade_educational_institution.id)
--     WHERE student_grade_educational_institution.created_at in (select max(created_at) from student_grade_educational_institution group by student_id)
-- ) TO '/tmp/export_34625/student_16.11.csv' DELIMITER ',' CSV HEADER;

-- UPD: this table is used solely for grades
COPY (
    SELECT
    student.id,
    educational_institution_id,
    value as "grade",
    student.is_deleted OR sgei.is_deleted as "is_deleted"
    FROM
    student
    LEFT JOIN student_grade_educational_institution sgei on (student.id = sgei.student_id)
    LEFT JOIN grade_educational_institution on (sgei.grade_educational_institution_id = grade_educational_institution.id)
    LEFT JOIN grade on grade_educational_institution.grade_id = grade.id
    WHERE sgei.created_at = (select max(created_at) from student_grade_educational_institution as sgei1 where sgei.student_id=sgei1.student_id)
) TO '/tmp/export_34625/student_16.11.csv' DELIMITER ',' CSV HEADER;

COPY (
    SELECT
    eins.region AS "????????????",
    eins.short_name AS "??????????",
    eins.inn AS "??????",
    eins.address AS "??????????",
    pei.profile_id as "profile_id",
    pei.approved_status as "approved_status",
    pei.role as "role"
    FROM (
        SELECT DISTINCT smartcode_id FROM (
            SELECT smartcode_id FROM smartcode_external_system WHERE is_deleted = false
        ) AS not_deleted
    ) AS activated
    LEFT JOIN smartcode ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
) TO '/tmp/export_34625/school_students.csv' DELIMITER ',' CSV HEADER;

COPY (
    SELECT
    eins.region AS "????????????",
    eins.short_name AS "??????????",
    eins.inn AS "??????",
    COUNT(CASE WHEN pei.role = 'STUDENT' THEN 1 ELSE NULL END) AS "?????????? ????????????????",
    COUNT(CASE WHEN pei.role = 'STUDENT' AND pei.approved_status = 'NONE' THEN 1 ELSE NULL END) AS "???? ???????????????????????????? ??????????????",
    COUNT(CASE WHEN pei.role = 'STUDENT' AND pei.approved_status = 'NOT_APPROVED' THEN 1 ELSE NULL END) AS "?????????????????????? ??????????????",
    COUNT(CASE WHEN pei.role = 'STUDENT' AND pei.approved_status = 'APPROVED' THEN 1 ELSE NULL END) AS "???????????????????????????? ??????????????",
    COUNT(CASE WHEN pei.role = 'TEACHER' THEN 1 ELSE NULL END) AS "?????????? ????????????????????????????",
    COUNT(CASE WHEN pei.role = 'TEACHER' AND pei.approved_status = 'NONE' THEN 1 ELSE NULL END) AS "???? ???????????????????????????? ??????????????????????????",
    COUNT(CASE WHEN pei.role = 'TEACHER' AND pei.approved_status = 'NOT_APPROVED' THEN 1 ELSE NULL END) AS "?????????????????????? ??????????????????????????",
    COUNT(CASE WHEN pei.role = 'TEACHER' AND pei.approved_status = 'APPROVED' THEN 1 ELSE NULL END) AS "???????????????????????????? ??????????????????????????"
    FROM profile_educational_institution AS pei
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
    WHERE pei.is_deleted = false
    GROUP BY eins.region, eins.short_name, eins.inn
) TO '/tmp/export_34625/????????????????????????_????_????????????.csv' DELIMITER ',' CSV HEADER;



COPY (
    SELECT
    pei.approved_status AS "????????????",
    COUNT(1) AS "?????????? ????????????????????????",
    COUNT(CASE WHEN pei.role='STUDENT' THEN 1 ELSE NULL END) AS "??????????????????", COUNT(CASE WHEN pei.role='TEACHER' THEN 1 ELSE NULL END) AS "????????????????????????????" FROM
    (SELECT smartcode_id
    FROM smartcode_external_system GROUP BY smartcode_id)
    AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    WHERE (pei.role = 'STUDENT' OR pei.role='TEACHER') AND pei.is_deleted = 'f' GROUP BY approved_status
) TO '/tmp/export_34625/????????????????_??_????????????????????????????_????_????????????????_??????????????????????????.csv' DELIMITER ',' CSV HEADER;


COPY (
    SELECT
    external_system.short_name AS platform_name,
    COUNT(1) AS "????????????????????????",
    COUNT(CASE WHEN pei.approved_status='APPROVED' THEN 1 ELSE NULL END) AS "????????????????????????" FROM smartcode_external_system AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei
    ON pei.profile_id = smartcode.profile_id
    LEFT JOIN external_system
    ON activated.system_code = external_system.system_code
    WHERE pei.role = 'STUDENT' OR pei.role='TEACHER'
    GROUP BY platform_name
) TO '/tmp/export_34625/????????????????????????_????_????????????????????.csv' DELIMITER ',' CSV HEADER;


COPY (
    SELECT
    DATE(activated.created_at) AS date_activated,
    COUNT(CASE WHEN pei.role='STUDENT' THEN 1 ELSE NULL END) AS "??????????????????", COUNT(CASE WHEN pei.role='TEACHER' THEN 1 ELSE NULL END) AS "????????????????????????????" FROM
    (SELECT smartcode_id, MIN(created_at) AS created_at FROM smartcode_external_system
    GROUP BY smartcode_id)
    AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = smartcode.profile_id
    GROUP BY date_activated
    ORDER BY date_activated
) TO '/tmp/export_34625/??????????????_????????????????_??_????????????????.csv' DELIMITER ',' CSV HEADER;


COPY (
    SELECT
    profile.full_name as "??????",
    profile.email as "Email",
    profile.phone as "??????????????",
    region as "????????????",
    municipal_area as "??????????????????????????",
    ei.short_name as "??????????",
    ei.inn as "??????",
    CASE WHEN pei.approved_status = 'APPROVED' THEN '??????????????????????'
        WHEN pei.approved_status = 'NONE' THEN '?? ????????????????'
        WHEN pei.approved_status = 'NOT_APPROVED'
        THEN '????????????????' END AS "????????????"
    FROM profile_educational_institution as pei
    LEFT JOIN profile ON profile.id = pei.profile_id
    LEFT JOIN educational_institution as ei ON ei.id = pei.educational_institution_id
    WHERE pei.role = 'TEACHER'
) TO '/tmp/export_34625/teachers_14_12.csv' DELIMITER ',' CSV HEADER;


COPY (
    WITH started_approving as (SELECT DISTINCT confirmator_id as approving from profile_educational_confirmation_log)
    SELECT
    eins.region AS "????????????",
    eins.short_name AS "??????????",
    eins.inn AS "??????",
    eins.address AS "??????????",
    profile.full_name as "??????",
    profile.phone as "??????????????",
    CASE WHEN scos_response.approve_status = 'SCOS' or scos_response.approve_status = 'MANUAL' THEN 1 ELSE NULL END as "??????????????????????",
    CASE WHEN started_approving.approving IS NOT NULL THEN 1 ELSE NULL END as "???????????? ????????????????????????"
    FROM
    profile
    LEFT JOIN profile_educational_institution AS pei ON pei.profile_id = profile.id
    LEFT JOIN educational_institution AS eins ON pei.educational_institution_id = eins.id
    LEFT JOIN scos_response ON profile.id = scos_response.profile_id
    LEFT JOIN started_approving ON profile.id = started_approving.approving
    where pei.role = 'INSTITUTE'
) TO '/tmp/export_34625/director_approving.csv' DELIMITER ',' CSV HEADER;

COPY (
    SELECT
    external_system.short_name AS platform_name,
    pei.profile_id,
    pei.approved_status,
    pei.role,
    pei.educational_institution_id,
    eins.short_name,
    eins.inn,
    eins.region,
    eins.locality,
    eins.municipal_area
    FROM smartcode_external_system AS activated
    LEFT JOIN smartcode
    ON activated.smartcode_id = smartcode.id
    LEFT JOIN profile_educational_institution AS pei
    ON pei.profile_id = smartcode.profile_id
    LEFT JOIN educational_institution AS eins
    ON pei.educational_institution_id = eins.id
    LEFT JOIN external_system
    ON activated.system_code = external_system.system_code
    WHERE external_system.short_name = '????????????????'
) TO '/tmp/export_34625/foxford.csv' DELIMITER ',' CSV HEADER;