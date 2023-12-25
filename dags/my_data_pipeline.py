import os
import json
import smtplib
import requests
import psycopg2
import pandas as pd
from typing import Dict, List
from dotenv import load_dotenv
from psycopg2.extensions import connection
from psycopg2.extras import execute_values

# Load environment variables
load_dotenv()

# Lista de ciudades
cities = ["amsterdam", "berlin", "paris"]

# Contaminantes para los que se generará un DataFrame
contaminants = ['o3', 'pm10', 'pm25', 'uvi']


def fetch_air_quality(city: str) -> dict:
    """
    Fetch air quality data for a given city.
    
    Args:
        city (str): The name of the city.

    Returns:
        dict: The air quality data.
    """
    print(f"Fetching air quality for {city}")

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

def create_dataframe_for_contaminant(contaminant: str, cities_data: Dict[str, dict]) -> pd.DataFrame:
    """
    Creates a pandas DataFrame for a given contaminant based on the air quality data of multiple cities.
    
    Args:
        contaminant (str): The contaminant for which the DataFrame is to be created.
        cities_data (Dict[str, dict]): A dictionary with city names as keys and their air quality data as values.

    Returns:
        pd.DataFrame: The resulting DataFrame with columns for day, country, and statistics of the contaminant.
    """
    data_list: List[dict] = []  # List to store data rows for the DataFrame
    for city, city_data in cities_data.items():
        # Check if the data for the specific contaminant is available
        if 'forecast' in city_data and 'daily' in city_data['forecast'] and contaminant in city_data['forecast']['daily']:
            for forecast in city_data['forecast']['daily'][contaminant]:
                # Append the data for each day into the data list
                data_list.append({
                    'day': forecast['day'],
                    'country': city.capitalize(),
                    f'{contaminant}_daily_avg': forecast['avg'],
                    f'{contaminant}_daily_max': forecast['max'],
                    f'{contaminant}_daily_min': forecast['min']
                })
    # Convert the list of data into a pandas DataFrame
    return pd.DataFrame(data_list)

# Collect air quality data for each city
cities_data = {city: fetch_air_quality(city) for city in cities}

# Dictionary to store the DataFrames of each pollutant
dataframes = {}

# Create a DataFrame for each contaminant.
for contaminant in contaminants:
    df = create_dataframe_for_contaminant(contaminant, cities_data)
    dataframes[contaminant] = df
    print(f"DataFrame para {contaminant}:")
    print(df)
    print("-" * 40)

def cargar_en_redshift(conn: connection, table_name: str, dataframe: pd.DataFrame):
    """
    Loads data into a Redshift table from a pandas DataFrame.

    Args:
        conn (connection): The psycopg2 connection object to Redshift.
        table_name (str): The name of the table where data will be loaded.
        dataframe (pd.DataFrame): The DataFrame containing the data to load.
    """
    # Map the DataFrame dtypes to Redshift data types
    type_map = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(50)', 'bool': 'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in dataframe.dtypes.values]
    column_defs = [f'"{name}" {data_type}' for name, data_type in zip(dataframe.columns, sql_dtypes)]
    
    # Create table schema
    table_schema = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)});"
    cur = conn.cursor()
    cur.execute(table_schema)
    
    # Insert data
    insert_sql = f"INSERT INTO {table_name} ({', '.join(['"' + col + '"' for col in dataframe.columns])}) VALUES %s"
    values = [tuple(x) for x in dataframe.to_numpy()]
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Data loaded into Redshift table successfully.')

# Function to connect to Redshift
def connect_to_redshift() -> connection:
    """
    Establishes a connection to a Redshift database.

    Returns:
        connection: A psycopg2 connection object to the Redshift database.
    """
    # Get database connection parameters from environment variables
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_port = os.getenv("DB_PORT")
    db_password = os.getenv("DB_PASSWORD_FILE")
    
    try:
        # Establish connection to Redshift
        conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password, port=db_port)
        print("Connected to Redshift successfully!")
        return conn
    except Exception as e:
        print("Unable to connect to Redshift:", e)
        return None
    
def fetch_and_process_data() -> Dict[str, dict]:
    """
    Fetches and processes air quality data for a predefined list of cities.

    Returns:
        Dict[str, dict]: A dictionary with contaminants as keys and their respective data as values.
    """
    # Define the list of cities to fetch data for
    cities = ["amsterdam", "berlin", "paris"]
    # Define the list of contaminants
    contaminants = ['o3', 'pm10', 'pm25', 'uvi']
    
    # Fetch air quality data for each city
    cities_data = {city: fetch_air_quality(city) for city in cities}
    
    # Process the data and create a dictionary of DataFrames for each contaminant
    dataframes = {contaminant: create_dataframe_for_contaminant(contaminant, cities_data) for contaminant in contaminants}
    
    # Convert DataFrames to a list of dictionaries
    return {contaminant: df.to_dict(orient='records') for contaminant, df in dataframes.items()}

def load_data_to_redshift(dataframes: Dict[str, pd.DataFrame]):
    """
    Loads processed air quality data into Redshift tables.

    Args:
        dataframes (Dict[str, pd.DataFrame]): A dictionary with contaminants as keys and their respective data as pandas DataFrames.
    """
    conn = connect_to_redshift()
    if conn:
        # Load each DataFrame into a Redshift table
        for contaminant, df in dataframes.items():
            table_name = f"air_quality_{contaminant}"
            cargar_en_redshift(conn, table_name, df)
        conn.close()

def enviar_email(subject: str, body_text: str, recipient: str):
    """
    Sends an email with the provided subject and body text to the specified recipient.
    
    Args:
        subject (str): The subject of the email.
        body_text (str): The body text of the email.
        recipient (str): The recipient's email address.
    """
    try:
        password = os.getenv("GMAIL_PASS")
        
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login('franciscofurey@gmail.com', password)
            message = f'Subject: {subject}\n\n{body_text}'
            server.sendmail('franciscofurey@gmail.com', recipient, message)
            print('Email enviado con éxito')
    except Exception as e:
        print(f'Error al enviar email: {e}')

def prepare_email_content(dataframes):
    """
    Prepare the body text for the email with a summary of processed data.
    
    Args:
        dataframes (dict): A dictionary of pandas DataFrames with air quality data.
    
    Returns:
        str: The body text for the email.
    """
    body_lines = []
    for contaminant, df in dataframes.items():
        body_lines.append(f"Resumen para el contaminante {contaminant}:")
        summary = df.describe().to_string()  # Summarize the DataFrame statistics
        body_lines.append(summary)
        body_lines.append("")  # Add an empty line for spacing

    return "\n".join(body_lines)

def send_summary_email(dataframes: Dict[str, pd.DataFrame], recipient: str, email_subject: str = "Resumen de Calidad del Aire"):
    """
    Sends an email summary of the air quality data processing results.

    Args:
        dataframes (Dict[str, pd.DataFrame]): A dictionary containing the air quality data for each contaminant.
        recipient (str): The recipient's email address.
        email_subject (str): The subject of the email. Defaults to "Resumen de Calidad del Aire".
    """
    # Prepare the body text of the email
    body_text = prepare_email_content(dataframes)
    
    # Send the email
    enviar_email(subject=email_subject, body_text=body_text, recipient=recipient)
