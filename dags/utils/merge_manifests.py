from pathlib import Path
import json
from airflow.decorators import task
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException

from utils.jinja_utils import render_template, save_config_to_file


def combine_all_manifests(staging_folder):
    manifest_file_path = f"/home/airflow/gcs/data/{staging_folder}/encrypted"
    decrypted_file_path = f"/home/airflow/gcs/data/{staging_folder}/decrypted"

    file_pattern = "*.manifest"

    # Merge all Manifests together
    files = []
    for manifest in Path(manifest_file_path).glob(file_pattern):
        with open(manifest) as f:
            files = files + json.load(f)['files']
    tables=[]
    print(files)
    file_dict={}
    bq_loader_dict = {}
    if files:
        file_dict = {file['cdhPath'] + '|' + file['file']: file['md5sum'] for file in files}
        tables = list(set(Path(table.split('|')[0]).name for table in file_dict.keys() if "hive" in table.split('|')[0] and "parquet" in table.split('|')[1]))

        for table in tables:
            files_list = []
            for file in file_dict:
                if table == Path(file.split('|')[0]).name:
                    files_list.append(f"{decrypted_file_path}{file.split('|')[0]}/{file.split('|')[1]}")
            bq_loader_dict[table] = files_list


    return bq_loader_dict, file_dict


@task.python(task_id="validate_md5")
def validate_md5(staging_folder):
    _, project_id = get_credentials_and_project_id()
    decrypted_file_path = f"/home/airflow/gcs/data/{staging_folder}/decrypted"
    scripts_path = f"/home/airflow/gcs/data/{staging_folder}/scripts"

    bq_loader_dict, file_dict = combine_all_manifests(staging_folder)

    # Read the md5 file generated on the cloud post decryption
    with open(f'{scripts_path}/md5sums.txt') as mdf:
        md5_data = json.load(mdf)

    print(md5_data)
    # Md5 Checks Manifest files against Cloud Computed
    md5_failures = []
    for key, value in file_dict.items():
        dap_file_path, parquet_file_name = key.split('|')
        if "parquet" in parquet_file_name and "hive" in dap_file_path:
            full_path_parquet_file_name=f"{decrypted_file_path}{dap_file_path}/{parquet_file_name}"
            if value != md5_data[full_path_parquet_file_name]:
                md5_failures.append(f"{dap_file_path}/{parquet_file_name}")

    if md5_failures:
        print(f'Error !! {len(md5_failures)} files have failed md5 Checks {md5_failures}')
        #raise AirflowException(f'The Following Files Failed md5 Checks {md5_failures}')


    print("----------------------------------------------------------------------------------------------------------")
    print("-------------------------------Hive Table(s) SUMMARY !!------------------------------------------------------")
    print("----------------------------------------------------------------------------------------------------------")
    for key, value in bq_loader_dict.items():
        print(f"Table:{key}, Num of Files:{len(value)}")


@task.python(task_id="gen_load_scripts")
def gen_load_scripts(staging_folder):
    # Generate Scripts to Move and Load Data
    scripts_path = f"/home/airflow/gcs/data/{staging_folder}/scripts"
    _, project_id = get_credentials_and_project_id()
    bq_loader_dict, file_dict = combine_all_manifests(staging_folder)

    # Create Script to Rearrange Files in Correct Directories
    context = get_current_context()
    ti = context["ti"]
    arrange_files = render_template(project=project_id,
                                    bq_loader_dict=bq_loader_dict,
                                    template_name="arrange_files.jinja",
                                    staging_folder=staging_folder)

    save_config_to_file(config=arrange_files,
                        config_dir=Path(scripts_path),
                        file_name="group_files_by_table.sh")

    # Load to BQ
    bq_loader_script = render_template(project=project_id,
                                       bq_loader_dict=bq_loader_dict,
                                       template_name="load_bq.jinja",
                                       dataset="demo",
                                       staging_folder=staging_folder)

    save_config_to_file(config=bq_loader_script,
                        config_dir=Path(scripts_path),
                        file_name="load_parquets_to_bq.sh")

