from bkenganaf_ns4_zwtm_cgw_.utils import *

def S3FileSensor_1():
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    from datetime import timedelta

    return S3KeySensor(
        task_id = "S3FileSensor_1",
        bucket_key = [s.strip() for s in "test/validation_data/test_source.json".split(",") if s.strip()],
        bucket_name = "qa-prophecy",
        check_fn = None,
        aws_conn_id = "6V76fxalxxOPuRpIVo7_d",
        wildcard_match = False,
        verify = False,
        timeout = 600,
    )
