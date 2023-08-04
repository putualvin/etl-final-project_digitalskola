from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import smtplib
import pendulum
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,7,22),
    'email':['putu@migo.io'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    dag_id='final_project_testing',
    default_args=default_args,
    description='Update sales hourly',
    catchup=False,
    schedule_interval='@daily',
)
get_data_from_api = BashOperator(
    task_id='get_data_from_api',
    bash_command='python3 /root/final-project/extract.py',
    dag=dag
)
generate_dim = BashOperator(
    task_id='generate_dim',
    bash_command='python3 /root/final-project/generate_dim.py',
    dag=dag
)

insert_district_daily = BashOperator(
    task_id='insert_district_daily',
    bash_command='python3 /root/final-project/insert_district_daily.py',
    dag=dag
)
insert_province_daily = BashOperator(
    task_id='insert_province_daily',
    bash_command='python3 /root/final-project/insert_province_daily.py',
    dag=dag
)
get_data_from_api >> generate_dim >> insert_district_daily
get_data_from_api >> generate_dim >> insert_province_daily
