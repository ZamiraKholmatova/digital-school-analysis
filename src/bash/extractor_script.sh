CREDENTIALS=$1
DST_PATH=$2

echo "Copying Files"
scp -P2221 -i $CREDENTIALS src/bash/export_tables.sh root@188.130.155.194:/root/export/
scp -P2221 -i $CREDENTIALS src/sql/epi.sql root@188.130.155.194:/root/export/
scp -P2221 -i $CREDENTIALS src/sql/eci.sql root@188.130.155.194:/root/export/
echo "Exporting tables"
ssh root@188.130.155.194 -p2221 -i $CREDENTIALS "bash /root/export/export_tables.sh"
echo "Copying to local"
scp -P2221 -i $CREDENTIALS root@188.130.155.194:/root/export/exported/\{*.bz2,last_export\} $DST_PATH

#while true; do
#  echo "Exporting tables"
#  ssh root@188.130.155.194 -p2221 -i $CREDENTIALS "bash /root/export/export_tables.sh"
#  echo "Copying to local"
#  scp -P2221 -i $CREDENTIALS root@188.130.155.194:/root/export/exported/\{*.csv,last_export\} $DST_PATH
#  echo "Waiting"
#  sleep 7200
#done
