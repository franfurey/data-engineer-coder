import os
import json
import requests
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import execute_values

#from airflow.utils.dates import days_ago
import pandas as pd
import os

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="franciscofurey_coderhouse"
pwd="nFU3H5r7S8"

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}


def fetch_air_quality(city):
    print(f"Fetching air quality for {city}")
    # token = os.getenv("API_TOKEN")
    url = f"https://api.waqi.info/feed/{city}/?token=a5bb8b40338e56043cb70a20c6ff6fdb4f159b4a"
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.text)
        if data['status'] == 'ok':
            return data['data']
        else:
            return f"Error en la API: {data['status']}"
    elif response.status_code == 400:
        return "Solicitud no válida"
    elif response.status_code == 401:
        return "Token inválido"
    else:
        return f"Error: {response.status_code}"

# Lista de ciudades
cities = ["amsterdam", "berlin", "paris"]

# Contaminantes para los que se generará un DataFrame
contaminants = ['o3', 'pm10', 'pm25', 'uvi']

def create_dataframe_for_contaminant(contaminant, cities_data):
    print(f"Creating dataframe for {contaminant}")
    data_list = []
    for city, city_data in cities_data.items():
        if 'forecast' in city_data and 'daily' in city_data['forecast'] and contaminant in city_data['forecast']['daily']:
            for forecast in city_data['forecast']['daily'][contaminant]:
                data_list.append({
                    'day': forecast['day'],
                    'country': city.capitalize(),
                    f'{contaminant}_daily_avg': forecast['avg'],
                    f'{contaminant}_daily_max': forecast['max'],
                    f'{contaminant}_daily_min': forecast['min']
                })
    return pd.DataFrame(data_list)

# Recopilar datos de calidad del aire para cada ciudad
cities_data = {city: fetch_air_quality(city) for city in cities}

# Diccionario para almacenar los DataFrames de cada contaminante
dataframes = {}

# Crear un DataFrame para cada contaminante
for contaminant in contaminants:
    df = create_dataframe_for_contaminant(contaminant, cities_data)
    dataframes[contaminant] = df
    print(f"DataFrame para {contaminant}:")
    print(df)
    print("-" * 40)

def cargar_en_redshift(conn, table_name, dataframe):
    print(f"Loading data into Redshift table {table_name}")
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(50)', 'bool': 'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]

    # Encerrar los nombres de columna entre comillas dobles para manejar caracteres especiales
    column_defs = [f'"{name}" {data_type}' for name, data_type in zip(cols, sql_dtypes)]

    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    cur = conn.cursor()
    cur.execute(table_schema)
    values = [tuple(x) for x in dataframe.to_numpy()]

    # Encerrar los nombres de columna entre comillas dobles en la consulta SQL
    insert_sql = "INSERT INTO " + table_name + " (" + ", ".join(['"' + col + '"' for col in cols]) + ") VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')

# Función para conectarse a Redshift
def connect_to_redshift():
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    db_host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    db_name=redshift_conn["database"],
    db_user=redshift_conn["username"],
    db_port = '5439'
    db_password=redshift_conn["pwd"],
    # db_host = os.getenv("DB_HOST")
    # db_name = os.getenv("DB_NAME")
    # db_user = os.getenv("DB_USER")
    # db_port = os.getenv("DB_PORT")
    # db_password = os.getenv("DB_PASSWORD_FILE")

    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connected to Redshift successfully!")
        return conn
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
        return None
    

def fetch_and_process_data():
    # Recopilar y procesar datos
    cities_data = {city: fetch_air_quality(city) for city in ["amsterdam", "berlin", "paris"]}
    dataframes = {}
    for contaminant in ['o3', 'pm10', 'pm25', 'uvi']:
        df = create_dataframe_for_contaminant(contaminant, cities_data)
        # Convertir el DataFrame a una lista de diccionarios
        dataframes[contaminant] = df.to_dict(orient='records')
    return dataframes

def load_data_to_redshift(dataframes):
    conn = connect_to_redshift()
    if conn:
        for contaminant, df_dict in dataframes.items():
            # Reconstruir el DataFrame
            df = pd.DataFrame(df_dict)
            table_name = f"air_quality_{contaminant}"
            cargar_en_redshift(conn, table_name, df)
        conn.close()