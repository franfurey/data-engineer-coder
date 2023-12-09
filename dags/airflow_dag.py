from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from dags.my_data_pipeline import fetch_and_process_data, load_data_to_redshift

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Francisco',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    dag_id='air_quality_data_ingestion',
    default_args=default_args,
    description='Fetch and load air quality data into Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Tarea 1: Recuperar y procesar los datos
task_1 = PythonOperator(
    task_id='fetch_and_process_data',
    python_callable=fetch_and_process_data,
    dag=dag,
)

# Tarea 2: Cargar datos en Redshift
task_2 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    dag=dag,
)

# Definición del flujo de tareas
task_1 >> task_2
