from uitesting_shared_sql_childdatabricksshared_rel_db_mwaa.utils import *

def Script_1():
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_1", bash_command = "echo \"test56\"", )
