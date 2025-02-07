from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 7),  # 시작 날짜
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 생성
dag = DAG(
    'my_dag',  # DAG 이름
    default_args=default_args,
    description='My first DAG in Airflow',
    schedule_interval='@daily',  # 매일 실행
    catchup=False
)

# 실행할 함수 정의
def my_task():
    print("Hello! This is my first DAG.")

# Task 생성
task_1 = PythonOperator(
    task_id='print_hello',
    python_callable=my_task,
    dag=dag
)

task_1  # DAG 내에서 실행
