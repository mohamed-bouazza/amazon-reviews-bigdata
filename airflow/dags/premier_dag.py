from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'start_date': datetime(2025,10,8),
    'email': ['premier@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    "premier_dag_test",
    default_args=default_args,
    description="un simple dag de test notre premier dag",
    schedule_interval='0 0 1 * *',
    end_date=datetime(2026,4,13)
)

def print_hello():
    return 'hello world, c\'est mon premier dag'

start = EmptyOperator(task_id='start', dag=dag)
hello_world = PythonOperator(task_id='hello_world', python_callable=print_hello, dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

start >> hello_world >> end
