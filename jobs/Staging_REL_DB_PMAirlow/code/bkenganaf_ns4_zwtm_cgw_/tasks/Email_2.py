from bkenganaf_ns4_zwtm_cgw_.utils import *

def Email_2():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_2",
        to = "abhisheks@propehcy.io",
        subject = "Test Airflow",
        html_content = "Test Airflow",
        cc = "navneet@prophecy.io",
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "d8CjYosdwdveGN9xrwtU3",
    )
