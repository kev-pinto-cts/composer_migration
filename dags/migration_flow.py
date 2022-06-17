# Standard Imports
from pathlib import Path
# from utils.gen_copy_script import gen_copy_script, gen_spark_script
# from utils.merge_manifests import validate_md5, gen_load_scripts
from utils.parallel_decrypt_taskgroup import decrypt_hive_files

# Third Party Imports
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.edgemodifier import Label

ROOT_STAGING_FOLDER = "hive_migration"

default_args = {
    'start_date': days_ago(1),
    'catchup': False,
    'max_active_runs': 1,
    'tags': ["HDM", "Demo"],
    'retries': 0
}


@dag(dag_id=Path(__file__).stem,
     schedule_interval="@once",
     description=f"Historical Data Migration Demo",
     default_args=default_args)
def dag_main():
    scripts_path = f"/home/airflow/gcs/data/{ROOT_STAGING_FOLDER}/scripts"
    start = DummyOperator(task_id="start")

    init = BashOperator(task_id='init',
                        bash_command="scripts/hdm_init_script.sh",
                        params={'root_staging_folder': ROOT_STAGING_FOLDER})

    attach = BashOperator(task_id='attach',
                          bash_command=f"scripts/copy_data_to_airflow.sh",
                          params={'root_staging_folder': ROOT_STAGING_FOLDER})

    end_decrypt = DummyOperator(task_id="end_decrypt")

    calc_file_md5 = BashOperator(task_id='calc_file_md5',
                                 bash_command="scripts/calc_md5.sh",
                                 params={'ROOT_STAGING_FOLDER': ROOT_STAGING_FOLDER})

    # group_files_by_table = BashOperator(task_id='group_files_by_table',
    #                                     bash_command=f"{scripts_path}/group_files_by_table.sh ")
    #
    # setup_bq_dataset = BashOperator(task_id='setup_bq_dataset',
    #                                 bash_command="scripts/setup_dataset.sh",
    #                                 params={'ROOT_STAGING_FOLDER': ROOT_STAGING_FOLDER})
    #
    # run_spark = BashOperator(task_id='run_spark',
    #                          bash_command=f"/home/airflow/gcs/data/{ROOT_STAGING_FOLDER}/scripts/hdm_spark.sh ")

    start >> Label("Create Necessary Folders in Airflow Buckets") >> init >> Label(
        "Copy Data to Airflow buckets /data Subfolder") >> attach >> \
    Label("Decrypt in Parallel") >> decrypt_hive_files() >> end_decrypt >> Label("Calculate Md5s") >> calc_file_md5

    # >> Label("Compare Manifest MD5 with Calculated MD5s") >> \
    # validate_md5(ROOT_STAGING_FOLDER) >> gen_load_scripts(ROOT_STAGING_FOLDER) >> Label("Group Parquet Files 1 Folder Per Table ") >> group_files_by_table >> Label("Create Dataset if Not Already available") >> \
    # setup_bq_dataset >> gen_spark_script(ROOT_STAGING_FOLDER) >> Label("Run Serveless Spark Load Routine to Load Parquets to BQ")  >> run_spark


# Invoke DAG
dag = dag_main()
