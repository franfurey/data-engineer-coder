from sqlalchemy import create_engine
from psycopg2.extras import execute_values
import pandas as pd
import psycopg

with psycopg.connect(host="localhost", dbname='test', user='postgres',
                        password = 'secret',port='5439') as conn:
    with conn.cursor() as cur:
        # TODO: add DB name.
        conn = create_engine('postgresql://franciscofurey_coderhouse:nFU3H5r7S8@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')


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
