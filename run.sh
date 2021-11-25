#screen -dm -S "data_extractor" -L -Logfile "data_extractor.log" bash ./src/bash/extractor_script.sh ./credentials/id_rsa ./data/db_data
#sleep 600
#screen -dm -S "data_processing" -L -Logfile "processing.log" python src/python/calculating_costs2.py --billing ./data/billing/courses_and_platforms_proces.csv --student_grades ./data/db_data/student_16.11.csv --statistics_type ./data/db_data/statistic_type_16.11.csv --external_system ./data/db_data/external_system_16.11.csv --profile_educational_institution ./data/db_data/profile_educational_institution_16.11.csv --course_structure ./data/db_data/educational_courses_16.11.csv --course_structure_foxford ./data/FoxFord/course_structure_fox_ford.csv --course_types ./data/db_data/educational_course_type_16.11.csv --course_statistics ./data/db_data/educational_course_statistic_16.11.csv --course_statistics_foxford ./data/FoxFord/statistics --course_statistics_uchi ./data/uchi/statistics --last_export ./data/db_data/last_export --html_path ./html --resources_path ./resources
#screen -dm -S "http_server" -L -Logfile "http_server.log" python -m http.server -d ./html 8000

while true; do
  bash ./src/bash/extractor_script.sh ./credentials/id_rsa ./data/db_data
  python src/python/calculating_costs2.py --billing ./data/billing/courses_and_platforms_prices.csv --student_grades ./data/db_data/student_16.11.csv --statistics_type ./data/db_data/statistic_type_16.11.csv --external_system ./data/db_data/external_system_16.11.csv --profile_educational_institution ./data/db_data/profile_educational_institution_16.11.csv --course_structure ./data/db_data/educational_courses_16.11.csv --course_structure_foxford ./data/FoxFord/course_structure_fox_ford.csv --course_structure_meo ./data/meo/course_structure_meo.csv --course_types ./data/db_data/educational_course_type_16.11.csv --course_statistics ./data/db_data/educational_course_statistic_16.11.csv --course_statistics_foxford ./data/FoxFord/statistics --course_statistics_meo ./data/meo/statistics --course_statistics_uchi ./data/uchi/statistics --last_export ./data/db_data/last_export --html_path ./html --resources_path ./resources --region_info_path ./data/db_data/school_students.csv
  sleep 86400
done
