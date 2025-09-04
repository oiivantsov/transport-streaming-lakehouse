import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from datetime import datetime, timedelta
from spark_conn_conf import make_spark_task

default_args = {
    "owner": "OI",
    "start_date": datetime(2025, 4, 8),
    # "retries": 1,
    "retry_delay": timedelta(seconds=300),
}

with DAG(
    "batch_pipeline_stream",
    schedule="@daily",
    catchup=False,
    default_args=default_args,
):
    silver = make_spark_task("silver", "/home/jobs/staging/events_staging.py")
    gold   = make_spark_task("gold", "/home/jobs/dw/events_dw.py")

    silver >> gold
