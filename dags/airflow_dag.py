import os
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from my_data_pipeline import fetch_and_process_data, load_data_to_redshift, send_summary_email

dag_path = os.getcwd()

# Default arguments for the DAG
default_args = {
    'owner': 'Francisco Furey',
    'email':['franciscofurey@gmail.com'],
    'email_on_retry': True,
    'email_on_failure': True,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# Definition of DAG
print("Setting up the DAG")
dag = DAG(
    dag_id='air_quality_data_ingestion',
    default_args=default_args,
    description='Fetch and load air quality data into Redshift',
    schedule_interval=timedelta(days=1),
    tags = ['Primer Tag', 'Segundo Tag'],
    catchup=False
)
print("DAG setup complete")

# Task 1: Retrieve and process data
task_1 = PythonOperator(
    task_id='fetch_and_process_data',
    python_callable=fetch_and_process_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Load data in Redshift
task_2 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    provide_context=True,
    op_kwargs={'dataframes': "{{ task_instance.xcom_pull(task_ids='fetch_and_process_data') }}"},
    dag=dag,
)

# Task 3: Sending e-mails in the DAG
send_email_task = PythonOperator(
    task_id='send_summary_email',
    python_callable=send_summary_email,
    op_kwargs={
        'dataframes': "{{ ti.xcom_pull(task_ids='fetch_and_process_data') }}",
        'recipient': 'franciscofurey@gmail.com',
        'email_subject': 'Resumen de Calidad del Aire',
    },
    dag=dag,
)
# Definición del flujo de tareas
task_1 >> task_2 >> send_email_task