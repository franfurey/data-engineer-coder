import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from my_data_pipeline import fetch_and_process_data, load_data_to_redshift

dag_path = os.getcwd()

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Francisco Furey',
    'email':['franciscofurey@gmail.com'],
    'email_on_retry': True,
    'email_on_failure': True,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# Definición del DAG
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

# Tarea 1: Recuperar y procesar los datos
task_1 = PythonOperator(
    task_id='fetch_and_process_data',
    python_callable=fetch_and_process_data,
    provide_context=True,
    dag=dag,
)

# Tarea 2: Cargar datos en Redshift
task_2 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    provide_context=True,
    op_kwargs={'dataframes': "{{ task_instance.xcom_pull(task_ids='fetch_and_process_data') }}"},
    dag=dag,
)

# Definición del flujo de tareas
task_1 >> task_2