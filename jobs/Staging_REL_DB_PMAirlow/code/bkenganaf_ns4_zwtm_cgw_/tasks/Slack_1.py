from bkenganaf_ns4_zwtm_cgw_.utils import *

def Slack_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1",
        text = "Test Slack message DB Airflow PM",
        channel = "abhyslackpub",
        slack_conn_id = "qVPMLyIRC9HnFusM3vDBk",
    )
