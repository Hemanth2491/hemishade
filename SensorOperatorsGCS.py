import datetime

from airflow import models

from airflow.operators.dummy.operator import DummyOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucket
from airflow.providers.google.cloud.operators.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from datetime import date, timedelta

from airflow.models import Variable

#project_id="hemanthde2024"
#region="US-Central1"

project_id = Variable.get('project_id')
region = Variable.get('region')
source_bucket_name=Variable.get('source_bucket_name')
today = datetime.datetime.today()
file_prefix="source/dummy_"
file_suffix ="csv"
file_date=today.strftime('%y-%m-%d')
file_name=file_prefix+file_date+file_suffix

default_args = {
    "project_id"
}

with models.DAG(
    "SensorOperatorGCSCopy"
    default_args = {
     # Tell airflow to start one day ago, so that it runs as soon as you upload it
        "project_id":project_id
        "region": region
        "starttime": days_ago(1)
    },
    Schedule_Interval=datetime.timedelta(days=1),
) as dag:

start_task = DummyOperator(
    task_id="start_task",
    dag=dag,
)

sensor_obj = GoogleCloudStorageObjectSensor(
    task_id = "Sensor_check",
    bucket_name= source_bucket_name,
    object=file_name,
    timeout=120,       #Set a timeout seconds
    poke_interval=10,  #How Often to check for the object(in Seconds)
    mode='poke',       #Use 'reschedule' if you want to retries with backoff
)

create_bucket = GCSCreateBucket(
    task_id="create_bucket",
    bucket_name='hemisha2018bcomp1',
)

copyGCSbucketfile = GCSToGCSOperator(
    task_id="copy_files",
    bucket_name=source_bucket_name,
    source_object="source/*.csv",
    target_bucket="hemisha2018bcomp1",
)
end_task = DummyOperator(
    task_id="end_task",
    dag=dag,
)

start_task>>sensor_obj>>create_bucket>>copyGCSbucketfile>>end_task
