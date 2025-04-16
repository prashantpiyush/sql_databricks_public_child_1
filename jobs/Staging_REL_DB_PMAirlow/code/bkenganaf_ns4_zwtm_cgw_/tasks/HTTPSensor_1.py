from bkenganaf_ns4_zwtm_cgw_.utils import *

def HTTPSensor_1():
    from airflow.providers.http.sensors.http import HttpSensor
    from datetime import timedelta

    # Execution timeout is airflow task level execution timeout
    # Sensor timeout will be different. Should be handled separately
    return HttpSensor(
        task_id = "HTTPSensor_1",
        endpoint = "/webhp",
        request_params = None,
        headers = None,
        response_check = None,
        http_conn_id = "qg4zfoFh98cOx-J9Zkgyt",
        poke_interval = 5,
        timeout = 600,
    )
