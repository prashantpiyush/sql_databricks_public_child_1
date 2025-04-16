from uitesting_shared_team_sql_databrickssharedbasic_rel_db_airflow.utils import *

def Script_2():
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_2", bash_command = "date", )
