from uitesting_shared_sql_childdatabricksshared_rel_db_mwaa.utils import *

def Script_2():
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_2", bash_command = "date", )
