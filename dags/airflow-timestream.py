
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import awswrangler as wr


#replace this with airflow date values so they remain tied to when the dag is run/scheduled
todays_date = datetime.now()
s3folder = todays_date.strftime("%Y%m%d%H%M")
print (s3folder)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    # 'end_date': datetime(2016, 1, 1),
}

def ts_query():
    wr.s3.to_csv(
    df=wr.timestream.query("""
SELECT region, availability_zone, instance_name, BIN(time, 24h) AS binned_timestamp,
    ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization
FROM "demoAirflow".kinesisdata1
WHERE measure_name = 'cpu_system'
    AND time > ago(24h)
GROUP BY region, instance_name, availability_zone, BIN(time, 24h)
ORDER BY binned_timestamp ASC
"""),
    path='s3://demo-airflow-ts-output/{s3folder}/my_file.csv',
).format(s3folder=s3folder)

with DAG(
        dag_id=os.path.basename(__file__).replace(".py", ""),
        default_args=default_args,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval=None
) as dag:

    ts_query=PythonOperator(task_id='ts_query', python_callable=ts_query, dag=dag)
    ts_query
