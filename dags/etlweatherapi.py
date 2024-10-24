from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import requests
import json


LAT = '51.5073'
LON = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# DAG definition

with DAG(
    dag_id='etl_pipeline_weatherapi',
    default_args = default_args,
    schedule_interval='@daily',
    catchup=False) as dags:
    
    @task()
    def extract_weather_api():
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        endpoint = f'/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true&temperature_unit=celsius'
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'HTTP status code: {response.status_code}')
        
    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LAT,
            'longitude': LON,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        ## Creation of table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT, 
                longitude FLOAT, 
                temperature FLOAT, 
                windspeed FLOAT, 
                winddirection FLOAT, 
                weather_code INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            """)
        
        ## Insertion of transformed data into the table
        cursor.execute(
            """
            INSERT INTO weather_data( latitude, longitude, temperature, windspeed, winddirection, weather_code) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                transformed_data['latitude'], 
                transformed_data['longitude'], 
                transformed_data['temperature'], 
                transformed_data['windspeed'], 
                transformed_data['winddirection'], 
                transformed_data['weathercode']
                ))
        
        conn.commit()
        cursor.close()
        
    ## DAG WORKFLOW - ETL PIPELINE WEATHERAPI
    weather_data = extract_weather_api()
    transformed_weather_data = transform_weather_data(weather_data)
    load_weather_data(transformed_weather_data)
    # weather_data >> transformed_weather_data >> load_weather_data