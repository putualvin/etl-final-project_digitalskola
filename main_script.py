from sqlalchemy import create_engine
import json
import psycopg2
import requests
import pandas as pd
import mysql.connector
import pymysql
import os


class DataExtractor():
    def __init__(self, api_url, content_type = 'appliaction/json'):
        self.api_url = api_url
        self.content_type = content_type
    
    def api_request(self, path, params = None, headers=None):
        try:
            url = f'{self.api_url}/{path}'
            response = requests.get(url, params=params, headers=headers)
            
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f'Error: Unable to fetch data from API. {e}')
            
    def json_to_dataframe(self, path, params=None, headers =None):
        try:
            json_data = self.api_request(path, params = params, headers= headers)
            data = json_data['data']['content']
            df = pd.DataFrame(data)
            return df
        except requests.exceptions.RequestException as e:
            raise Exception(f'Error: Unable to fetch data from api. {e}')

class DatabaseConnection():
    def __init__(self):
        pass
    
    def config(self, connection):
        path = os.getcwd()
        with open(path+'/'+ 'config.json') as file:
            conf = json.load(file)[connection]
        return conf
    
    def connect_to_postgres(self, conf):
        try:
            conn = psycopg2.connect(
                host = conf['host'],
                database = conf['db'],
                user = conf['user'],
                password = conf['password'],
                port = conf['port']
            )
            cur = conn.cursor()
            conn.set_session(autocommit=True)
            return conn, cur
        except psycopg2.Error as e:
            raise Exception(f'Error: Unable to connect to Postgresql. {e}')
    
    def connect_to_mysql(self, conf):
        try:
            conn = mysql.connector.connect(
                host = conf['host'],
                database = conf['db'],
                user = conf['user'],
                password = conf['password'],
                port = conf['port']
            )
            cur = conn.cursor()
            conn.autocommit = True
            return conn, cur
        except mysql.connector.Error as e:
            raise Exception(f'Error: Unable to connect to mysql. {e}')
    
class CreateData():
    def __init__(self):
        pass
    
    def store_to_mysql(self, conf, data, table_name):
        try:
            engine = create_engine(f"mysql+pymysql://{conf['user']}:{conf['password']}@{conf['host']}/{conf['db']}")
            data.to_sql(name = table_name,
                        con = engine,
                        if_exists = 'replace',
                        index = False)
        except Exception as e:
            print(f'Error: {e}. Cant connect to database')
    
    def create_dim_table(self, conn, cur ,conf, sql_file,file_name):
        try:
            engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['db']}")
            with open(sql_file, 'r') as file:
                file_sql = file.read()
            cur.execute(file_sql)
            result = cur.fetchall()
            col = [desc[0] for desc in cur.description]
            df = pd.DataFrame(result, columns = col)
            df.to_sql(
                name = file_name,
                con = engine,
                if_exists='replace',
                index = False,
                schema= 'public'
            )
        except Exception as e:
            print(f'Error: {e}')
        

    def create_table(self, conn, cur, conf, sql_file, file_name):
        with open(sql_file, 'r') as file:
            file_sql = file.read()
        cur.execute(file_sql)
        
        
        
        