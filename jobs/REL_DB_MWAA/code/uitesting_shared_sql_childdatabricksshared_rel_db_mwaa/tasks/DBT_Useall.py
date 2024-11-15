from uitesting_shared_sql_childdatabricksshared_rel_db_mwaa.utils import *

def DBT_Useall():
    from airflow.operators.python import PythonOperator
    from datetime import timedelta
    import os
    import zipfile
    import tempfile

    return PythonOperator(
        task_id = "DBT_Useall",
        python_callable = invoke_dbt_runner,
        op_kwargs = {
          "is_adhoc_run_from_same_project": False,
          "is_prophecy_managed": False,
          "run_deps": True,
          "run_seeds": False,
          "run_parents": False,
          "run_children": False,
          "run_tests": False,
          "run_mode": "model",
          "entity_kind": "model",
          "entity_name": "env_uitesting_shared_useallmodel_1",
          "project_id": "74",
          "git_entity": "branch",
          "git_entity_value": "dev_staging",
          "git_ssh_url": "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1",
          "git_sub_path": "",
          "select": "",
          "threads": "2",
          "exclude": "",
          "run_props": " --profile run_profile",
          "envs": {"DBT_DATABRICKS_INVOCATION_ENV" : "prophecy", "DBT_PROFILES_DIR" : "/usr/local/airflow/dags"}
        },
        retry_exponential_backoff = True, 
        retries = 0
    )
