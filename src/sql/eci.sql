-- courses that are not deleted
COPY (
    SELECT id, external_id, system_code, course_name, grades from educational_courses where deleted = 'f'
) TO '/tmp/export_34625/educational_courses_only_courses_16.11.csv' DELIMITER ',' CSV HEADER;

-- all courses
COPY educational_courses(id,deleted,course_type_id,parent_id,external_link,external_id,external_parent_id,course_name,system_code)  TO '/tmp/export_34625/educational_courses_16.11.csv' DELIMITER ',' CSV HEADER;

-- course types (topic, subject, etc.)
COPY educational_course_type(id, type_name)  TO '/tmp/export_34625/educational_course_type_16.11.csv' DELIMITER ',' CSV HEADER;

-- statistics
-- COPY educational_course_statistic  TO '/tmp/export_34625/educational_course_statistic_16.11.csv' DELIMITER ',' CSV HEADER;
-- COPY (
--     SELECT
--     profile_id, educational_course_id, educational_course_statistic.created_at, statistic_type as "statistics_type"
--     FROM
--     educational_course_statistic
-- --     old_educational_course_statistic as educational_course_statistic
--     JOIN educational_courses ON educational_course_id = educational_courses.id
-- --     JOIN statistic_type ON statistic_type.id = statistic_type_id
--     WHERE
-- --     (
--         system_code = '0b37f22e-c46c-4d53-b0e7-8bdaaf51a8d0'::uuid OR
--         system_code = '3a4b37c1-1f7d-4cb9-b144-e24c708d9c20'::uuid OR
--         system_code = '13788b9a-3426-45b2-9ba5-d8cec8c03c0c'::uuid OR
--         system_code = 'd2735d92-6ad6-49c4-9b36-c3b16cee695d'::uuid OR
--         system_code = '61dbfd85-2f0b-49eb-ad60-343cc5f12a36'::uuid OR
--         system_code = '1d258153-7d01-4ed7-9035-3f9df9cf578f'::uuid
--     ) AND
-- --     entity_id = 2 or entity_id = 3
--     AND statistic_type = 'ENTER_TO_STUDY_SUBJECT'
--     ORDER BY educational_course_statistic.created_at
-- ) TO '/tmp/export_34625/educational_course_statistic_16.11.csv' DELIMITER ',' CSV HEADER;

-- statistics type description
COPY statistic_type  TO '/tmp/export_34625/statistic_type_16.11.csv' DELIMITER ',' CSV HEADER;

COPY(
	SELECT
	profile_id,
	course_name,
	system_code
	FROM
	profile_paid_cok_course_2021
	LEFT JOIN educational_courses on profile_paid_cok_course_2021.cok_educational_course_id = educational_courses.id
)TO '/tmp/export_34625/payed2021.csv' DELIMITER ',' CSV HEADER;

COPY(
	SELECT
	ppacc.profile_id,
	course_name as "Наименование ЦОК",
	amount_pay as "payed_sum",
	period as "Месяц",
	educational_courses.system_code
	FROM profile_payment_amount_cok_course as ppacc
	LEFT JOIN educational_courses on ppacc.cok_educational_course_id = educational_courses.id
	WHERE period = '4.2022' or period = '3.2022' or period = '2.2022'
)TO '/tmp/export_34625/payed_this_year.csv' DELIMITER ',' CSV HEADER;

COPY(
	SELECT
	educational_courses.id,
	external_id,
	course_name,
	system_code,
	approved_date,
	cost_per_month as "price",
	max_cost as "max_price",
	(CASE WHEN educational_courses.approved='t' THEN 1 ELSE NULL END) AS "approved"
	FROM educational_courses
	LEFT JOIN cok_course_cost
	ON educational_courses.id = cok_educational_course_id
	WHERE deleted='f' AND educational_courses.approved='t' AND parent_id is null
)TO '/tmp/export_34625/courses.csv' DELIMITER ',' CSV HEADER;