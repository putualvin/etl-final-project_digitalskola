from main_script import DatabaseConnection, CreateData 
from datetime import datetime

def main():
    #Preparing class
    connection = DatabaseConnection()
    create_data = CreateData()

    #Create mysql connection and postgresql_connection
    conf_postgres = connection.config('postgres')
    conf_mysql = connection.config('mysql')

    #Creating province_case_number in mysql
    conn_mysql, cur_mysql = connection.connect_to_mysql(conf_mysql)
    conn_postgre, cur_postgre = connection.connect_to_postgres(conf_postgres)
    
    #Setting up table and exporting to postgresql
    start_district_case = datetime.now()
    print(f'Creating province_case_number at : {start_district_case}')
    conn_mysql_2, cur_mysql_2 = connection.connect_to_mysql(conf_mysql)
    create_data.create_dim_table(conn_mysql,
                                cur_mysql,
                                conf_postgres,
                                '/root/final-project/sql_script/province_case_number.sql',
                                'district')
    end_district_case = datetime.now()
    duration_district_case = end_district_case - start_district_case
    print(f'Successfully created province_case_number at {end_district_case}.. Duration: {duration_district_case}')
    

    #Creating aggregation in postgresql
    start_agg = datetime.now()
    print(f'Aggregation starts at {start_agg}')
    conn_postgre, cur_postgre = connection.connect_to_postgres(conf_postgres)
    create_data.create_dim_table(conn_postgre,
                                cur_postgre,
                                conf_postgres,
                                '/root/final-project/sql_script/fact_province_daily.sql',
                                'district_daily'
                                )
    end_agg = datetime.now()
    duration_agg = end_agg - start_agg
    print(f'Successfully created aggregation at {end_agg}. Duration: {duration_agg}')

if __name__ == '__main__':
    main()