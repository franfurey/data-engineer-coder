import os
import psycopg
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from psycopg2.extras import execute_values

with psycopg.connect(host="localhost", dbname='test', user='postgres',
                        password = 'secret',port='5439') as conn:
    with conn.cursor() as cur:
        # TODO: add DB name.
        # Cargar las variables de entorno
        load_dotenv()

        # Leer las variables de entorno
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD_FILE')  # Asumiendo que la contraseña está directamente en la variable
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_port = os.getenv('DB_PORT')

        # Formar la cadena de conexión utilizando las variables de entorno
        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'


        df = pd.read_sql_query('SELECT * FROM titanic', con = conn)

        conn2 = psycopg.connect(host="localhost", dbname='test', user='postgres',
                                password = 'secret',port='5439')

        cur = conn2.cursor

        cur.execute("CREATE TABLE test(id serial PRIMARY KEY, num integer, data varchar);")

        cur.execute("SELECT * FROM titanic;")

        result = cur.fetchone()

        cur.fetchall()

        conn.commit()

        cur.close()

        conn.close()
