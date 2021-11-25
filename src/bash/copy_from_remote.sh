CREDENTIALS=$1
DST_PATH=$2
scp -P2221 -i $CREDENTIALS root@188.130.155.194:/root/export/\{*16.11.csv,last_export\} $DST_PATH