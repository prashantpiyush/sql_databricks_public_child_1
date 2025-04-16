import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uitesting_shared_sql_childdatabricksshared_rel_db_mwaa.tasks import (
    DBT_SharedChildModel,
    DBT_Useall,
    Script_1,
    Script_2
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "uitesting_shared_SQL_ChildDatabricksShared_REL_DB_MWAA", 
    schedule_interval = "0 0 17 * *", 
    default_args = {
      "owner": "Prophecy", 
      "retries": 3, 
      "retry_delay": timedelta(minutes = 1.0), 
      "ignore_first_depends_on_past": True, 
      "do_xcom_push": True
    }, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2080, 1, 1, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    DBT_SharedChildModel_op = DBT_SharedChildModel()
    Script_1_op = Script_1()
    Script_2_op = Script_2()
    DBT_Useall_op = DBT_Useall()
    DBT_SharedChildModel_op >> Script_1_op
    Script_1_op >> [DBT_Useall_op, Script_2_op]
