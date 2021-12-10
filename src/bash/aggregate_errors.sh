STATISTICS_FOLDER=$1

#echo "profile_id\teducational_course_id\tcreated_at\n" > no_educational_course_id.tsv
#echo "profile_id\teducational_course_id\tcreated_at\n" > no_profile_id.tsv
echo "" > no_educational_course_id.tsv
echo "" > no_profile_id.tsv

cat $STATISTICS_FOLDER/*csv___edu* | sed 's/profile_id        educational_course_id   created_at//g' | grep "\S" >> $STATISTICS_FOLDER/no_educational_course_id.tsv
cat $STATISTICS_FOLDER/*csv___pro* | sed 's/profile_id        educational_course_id   created_at//g' | grep "\S" >> $STATISTICS_FOLDER/no_profile_id.tsv
cat $STATISTICS_FOLDER/*___unresolved___edu* | awk -F"\t" '{print $6}' | sed 's/educational_course_id_uuid//g' | grep "\S" | uniq | sort | uniq > $STATISTICS_FOLDER/unresolved_course_ids.txt
cat $STATISTICS_FOLDER/*___unresolved___pro* | awk -F"\t" '{print $5}' | sed 's/profile_id//g' | grep "\S" | uniq | sort | uniq > $STATISTICS_FOLDER/unresolved_profile_ids.txt