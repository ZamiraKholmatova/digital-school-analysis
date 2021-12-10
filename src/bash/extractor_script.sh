CREDENTIALS=$1
DST_PATH=$2

echo "Exporting tables"
ssh root@188.130.155.194 -p2221 -i $CREDENTIALS "bash /root/export/export_tables.sh"
echo "Copying to local"
scp -P2221 -i $CREDENTIALS root@188.130.155.194:/root/export/exported/\{*.csv,last_export\} $DST_PATH
python src/python/convert_db_data.py $DST_PATH

#while true; do
#  echo "Exporting tables"
#  ssh root@188.130.155.194 -p2221 -i $CREDENTIALS "bash /root/export/export_tables.sh"
#  echo "Copying to local"
#  scp -P2221 -i $CREDENTIALS root@188.130.155.194:/root/export/exported/\{*.csv,last_export\} $DST_PATH
#  echo "Waiting"
#  sleep 7200
#done
