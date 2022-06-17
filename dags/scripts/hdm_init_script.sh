#!/usr/bin/env bash
# Clean Up the Env
rm -rf /home/airflow/gcs/data/{{ params.root_staging_folder }}/||:
mkdir -p /home/airflow/gcs/data/{{ params.root_staging_folder }}/encrypted
mkdir -p /home/airflow/gcs/data/{{ params.root_staging_folder }}/decrypted
mkdir -p /home/airflow/gcs/data/{{ params.root_staging_folder }}/scripts
exit 0
