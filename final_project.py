import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import psycopg2
import mysql.connector

class DataExtractor():
    def __init__(self, api_url, content_type='application/json'):
        self.api_url = api_url
        self.content_type = content_type
    
    def api_request(self, path, params=None, headers=None):
        url = f'{self.api_url}/{path}'
        response = requests.get(url, params=params, headers=headers)
        return response.json()
        
    def json_to_dataframe(self, path, params=None, headers=None):
        try:
            start_time = datetime.now()
            json_data = self.api_request(path, params=params, headers=headers)
            data = json_data['data']['content']
            df = pd.DataFrame(data)
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"API Request completed in {duration.total_seconds():.2f} seconds.")
            return df
        except requests.exceptions.RequestException as e:
            raise Exception(f'Error: Unable to fetch data from the API. {e}')


class DatabaseConnection():
    def __init__(self):
        self.config_file = 'config.json'
        self.config_data = self.load_config()

    def load_config(self):
        path = os.getcwd()
        with open(path + '/'+self.config_file) as file:
            config_data = json.load(file)
        return config_data
    
    def get_connection_info(self, connection_type):
        return self.config_data[connection_type]

    def connect_to_database(self, connection_type):
        conf = self.get_connection_info(connection_type)
        if connection_type == 'postgres':
            conn = psycopg2.connect(
                host=conf['host'],
                database=conf['db'],
                user=conf['user'],
                password=conf['password'],
                port=conf['port']
            )
        elif connection_type == 'mysql':
            conn = mysql.connector.connect(
                host=conf['host'],
                database=conf['db'],
                user=conf['user'],
                password=conf['password']
            )
        else:
            raise ValueError('Unsupported database type in configuration.')
            
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        return conn, cur


class CreateData():
    def __init__(self, engine_from, engine_to):
        self.engine_from = engine_from
        self.engine_to = engine_to
    
    def transfer_data(self, sql_query):
        with self.engine_from.connect() as conn:
            start_time = datetime.now()
            df = pd.read_sql(sql_query, conn)
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"Data Transfer completed in {duration.total_seconds():.2f} seconds.")
        
        with self.engine_to.connect() as conn:
            start_time = datetime.now()
            df.to_sql('district_daily', conn, if_exists='replace', index=False, schema='public')
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"Data Insert completed in {duration.total_seconds():.2f} seconds.")


# def main():
#     api_url = 'https://api.example.com'  # Replace this with your API URL
#     connection = DatabaseConnection()
#     data_extractor = DataExtractor(api_url)

#     try:
#         # Load configuration and create database connections
#         conf_postgres = connection.get_connection_info('postgres')
#         conf_mysql = connection.get_connection_info('mysql')
#         engine_postgres = create_engine(
#             f"postgresql+psycopg2://{conf_postgres['user']}:{conf_postgres['password']}@{conf_postgres['host']}:{conf_postgres['port']}/{conf_postgres['db']}"
#         )
#         engine_mysql = create_engine(
#             f"mysql+pymysql://{conf_mysql['user']}:{conf_mysql['password']}@{conf_mysql['host']}/{conf_mysql['db']}"
#         )

#         # Get data from API
#         api_path = 'your_api_path'  # Replace this with the actual API path
#         start_time = datetime.now()
#         api_data = data_extractor.json_to_dataframe(api_path)
#         end_time = datetime.now()
#         duration = end_time - start_time
#         print(f"API Data Fetch completed in {duration.total_seconds():.2f} seconds.")

#         # Generating district_daily
#         print('Creating district_daily')
#         fact_district_query = '''
#         SELECT
#             dc.id,
#             dc.district_id,
#             c.id as case_id,
#             dc.tanggal::date::varchar as "date",
#             SUM(dc.total) as total
#         FROM digitalskola.disctrict_case_number dc
#         LEFT JOIN digitalskola.dim_case c ON dc.status_covid = c.status
#         GROUP BY 1, 2, 3, 4;
#         '''
#         create_data = CreateData(engine_mysql, engine_postgres)
#         create_data.transfer_data(fact_district_query)
#         print('Successfully created district_daily')
#     except Exception as e:
#         print(f"Error: {e}")


# if __name__ == "__main__":
#     main()