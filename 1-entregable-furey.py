import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

def fetch_air_quality(city):
    token = os.getenv("API_TOKEN")
    url = f"https://api.waqi.info/feed/{city}/?token={token}"
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

if __name__ == "__main__":
    cities = ["amsterdam", "berlin", "paris"]

    for city in cities:
        air_quality_data = fetch_air_quality(city)
        print(f"Datos de calidad del aire para {city.capitalize()}:")
        print(air_quality_data)
        print("-" * 40)  # Separador



import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # Cargar variables desde el archivo .env

# Leer variables de entorno
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_port = os.getenv("DB_PORT")
db_password_file = os.getenv("DB_PASSWORD_FILE")

# Leer la contraseña desde un archivo
with open(db_password_file, 'r') as f:
    db_password = f.read().strip()  # Utilizamos strip() para eliminar cualquier espacio en blanco o nueva línea adicional

# Intentar conectar
try:
    conn = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
    print("Connected to Redshift successfully!")
    
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)


import os
import requests
import json
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import psycopg2

load_dotenv()

def fetch_air_quality(city):
    token = os.getenv("API_TOKEN")
    url = f"https://api.waqi.info/feed/{city}/?token={token}"
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.text)
        if data['status'] == 'ok':
            return data['data']
    return None

def cargar_en_redshift(conn, table_name, dataframe):
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    cur = conn.cursor()
    cur.execute(table_schema)
    values = [tuple(x) for x in dataframe.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')

if __name__ == "__main__":
    cities = ["amsterdam", "berlin", "paris"]
    air_quality_data_list = []

    for city in cities:
        air_quality_data = fetch_air_quality(city)
        if air_quality_data:
            air_quality_data_list.append({
                'city': city,
                'aqi': air_quality_data.get('aqi'),
                'pm25': air_quality_data['iaqi'].get('pm25', {}).get('v'),
                'pm10': air_quality_data['iaqi'].get('pm10', {}).get('v')
            })

    df = pd.DataFrame(air_quality_data_list)
    
    # Aquí va el código para conectar a Redshift
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_port = os.getenv("DB_PORT")
    db_password_file = os.getenv("DB_PASSWORD_FILE")

    with open(db_password_file, 'r') as f:
        db_password = f.read().strip()

    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connected to Redshift successfully!")
        cargar_en_redshift(conn, 'air_quality', df)
        
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
