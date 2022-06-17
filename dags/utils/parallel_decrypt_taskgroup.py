from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

def decrypt_hive_files():
    with TaskGroup(group_id="decrypt_hive_files", add_suffix_on_collision=True, default_args={'retries': 0}) as decrypt_hive_files:
        decrypt_hive_files1 = BashOperator(task_id=f"decrypt_hive_files1",
                                       bash_command="scripts/decrypt_files_parallel.sh",
                                       params={'endpoint': "/home/airflow/gcs/data/hive_migration/encrypted/cdh/hive/table1"})
    return decrypt_hive_files
