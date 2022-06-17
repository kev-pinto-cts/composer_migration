from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

def decrypt_hive_files():
    with TaskGroup(group_id="decrypt_hive_files", add_suffix_on_collision=True, default_args={'retries': 0}) as decrypt_hive_files:
        decrypt_table1 = BashOperator(task_id=f"decrypt_table1",
                                       bash_command="scripts/decrypt_files_parallel.sh",
                                       params={'endpoint': "/home/airflow/gcs/data/hive_migration/encrypted/cdh/hive/table1"})

        decrypt_table2 = BashOperator(task_id=f"decrypt_table2",
                                           bash_command="scripts/decrypt_files_parallel.sh",
                                           params={
                                               'endpoint': "/home/airflow/gcs/data/hive_migration/encrypted/cdh/hive/table2"})

        decrypt_table3 = BashOperator(task_id=f"decrypt_table3",
                                           bash_command="scripts/decrypt_files_parallel.sh",
                                           params={
                                               'endpoint': "/home/airflow/gcs/data/hive_migration/encrypted/cdh/hive/table3"})

    return decrypt_hive_files
