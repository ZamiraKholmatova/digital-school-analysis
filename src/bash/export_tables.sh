docker exec -i db bash -c "if [ ! -d /tmp/export_34625 ]; then mkdir /tmp/export_34625 && chmod 777 /tmp/export_34625; fi"
cd /root/export &&
cat epi.sql | docker exec -i db psql -U postgres -d rco_auth_profile &&
cat eci.sql | docker exec -i db psql -U postgres -d stat_service &&
docker cp db:/tmp/export_34625/. /root/export/exported &&
echo "$(date +%Y-%m-%d_%H-%M-%S)" > /root/export/exported/last_export

for file in /root/export/exported/*.csv; do echo Compressing $file; bzip2 -f $file; done #copying, compressing files