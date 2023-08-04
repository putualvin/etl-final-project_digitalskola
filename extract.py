from datetime import datetime
from final_project import DataExtractor, DatabaseConnection, CreateData


def main():
    start_extract = datetime.now()
    print(f'Starting at : {start_extract}')
    #Defining parameters
    api_url = "http://103.150.197.96:5005"
    path = "/api/v1/rekapitulasi_v2/jabar/harian"
    content_type = "application/json"
    params = {"level": "provinsi"}
    headers = {"Content-Type": content_type}

    print('Starting data extractor')
    data_extractor = DataExtractor(api_url)
    df = data_extractor.json_to_dataframe(path, params=params, headers=headers)
    print('Succes creating data')
    
    #Creating connection
    connection = DatabaseConnection()
    create_data = CreateData()        
    config_mysql = connection.config('mysql')
    create_data.store_to_mysql(config_mysql,
                            df,
                            'covid_jabar')
    print("Successfully uploaded to mysql datalake")
    end_extract = datetime.now()
    duration_extract = end_extract - start_extract
    print(f'Extract ends at : {end_extract}. Duration {duration_extract}')
if __name__=='__main__':
    main()
