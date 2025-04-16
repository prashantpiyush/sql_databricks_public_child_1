import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uitesting_shared_team_sql_databrickssharedbasic_rel_db_airflow.tasks import DBT_0, DBT_Oauth, Python_1, Script_2
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "uitesting_shared_team_SQL_DatabricksSharedBasic_REL_DB_AIRFLOW", 
    schedule_interval = "0 0 * * 1", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    params = {
      'c_boolean': Param(False, type = "boolean", title = """c_boolean"""), 
      'c_object': Param(
        {"c_obj_str1" : "str1", "c_obj_str2" : "str2"}, 
        type = "object", 
        required = ["""c_obj_str1""", """c_obj_str2"""], 
        title = """c_object"""
      ), 
      'c_array': Param([], type = "array", uniqueItems = False, title = """c_array"""), 
      'branch': Param(
        """dev_staging""", 
        type = "string", 
        maxLength = 15, 
        minLength = 1, 
        pattern = """""", 
        title = """branch"""
      ), 
      'job_name': Param("""REL_DB_AIRFLOW""", type = "string", title = """job_name"""), 
      'c_int': Param("""2""", type = "string", title = """c_int"""), 
      'project_name': Param("""SQL_DatabricksSharedBasic""", type = "string", title = """project_name""")
    }, 
    start_date = pendulum.today('UTC'), 
    catchup = True, 
    tags = []
) as dag:
    DBT_0_op = DBT_0()
    Python_1_op = Python_1()
    Script_2_op = Script_2()
    DBT_Oauth_op = DBT_Oauth()
    DBT_0_op >> [DBT_Oauth_op, Python_1_op]
    Python_1_op >> Script_2_op
