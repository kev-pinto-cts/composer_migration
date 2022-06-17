#!/usr/bin/env bash

md5path="/home/airflow/gcs/data/hive_migration/scripts/md5sums.txt"

echo "{" > ${md5path}
find /home/airflow/gcs/data/hive_migration/decrypted/ -name "*.parquet" | while read fldr; do
    file_name=$(basename $fldr)
    md5sum $fldr |awk '{print "\""$2"\":\""$1"\","}' >>  ${md5path}
done
echo '"end":"end" }' >> ${md5path}
