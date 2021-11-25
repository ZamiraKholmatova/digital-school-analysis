docker exec -i db mkdir /tmp/export_34625 && docker exec -i db chmod 777 /tmp/export_34625
cd /root/export &&
cat epi.sql | docker exec -i db psql -U postgres -d rco_auth_profile &&
cat eci.sql | docker exec -i db psql -U postgres -d stat_service &&
docker cp db:/tmp/export_34625/. /root/export/exported &&
echo "$(date +%Y-%m-%d_%H-%M-%S)" > /root/export/last_export
#docker cp db:/tmp/export_34625/profile_role_16.11.csv /root/export/profile_role_16.11.csv &&
#docker cp db:/tmp/export_34625/role_16.11.csv /root/export/role_16.11.csv &&
#docker cp db:/tmp/export_34625/profile_educational_institution_16.11.csv /root/export/profile_educational_institution_16.11.csv &&
#docker cp db:/tmp/export_34625/external_system_16.11.csv /root/export/external_system_16.11.csv &&
#docker cp db:/tmp/export_34625/student_16.11.csv /root/export/student_16.11.csv &&
#docker cp db:/tmp/export_34625/educational_courses_only_courses_16.11.csv /root/export/educational_courses_only_courses_16.11.csv &&
#docker cp db:/tmp/export_34625/educational_courses_16.11.csv /root/export/educational_courses_16.11.csv &&
#docker cp db:/tmp/export_34625/educational_course_type_16.11.csv /root/export/educational_course_type_16.11.csv &&
#docker cp db:/tmp/export_34625/educational_course_statistic_16.11.csv /root/export/educational_course_statistic_16.11.csv &&
#docker cp db:/tmp/export_34625/statistic_type_16.11.csv /root/export/statistic_type_16.11.csv &&
#echo "$(date +%Y-%m-%d_%H-%M-%S)" > /root/export/last_export