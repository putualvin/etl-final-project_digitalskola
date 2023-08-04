from main_script import DatabaseConnection, CreateData
from datetime import datetime

def main():
    # Preparing Class Instances
    connection = DatabaseConnection()
    create_data = CreateData()

    # Creating connections
    conf_mysql = connection.config('mysql')
    conf_postgres = connection.config('postgres')
    conn_mysql, cur_mysql = connection.connect_to_mysql(conf_mysql)
    conn_postgres, cur_postgres = connection.connect_to_postgres(conf_postgres)

    # Creating dim_province
    start_dim_province = datetime.now()
    print(f'Start creating dim_province at {start_dim_province}')
    create_data.create_dim_table(conn_mysql,
                                cur_mysql,
                                conf_postgres,
                                '/root/final-project/sql_script/dim_province.sql',
                                'dim_province')
    end_dim_province = datetime.now()
    duration_dim_province = end_dim_province -start_dim_province
    print(f'Successfully created dim_province at {end_dim_province}. Duration: {duration_dim_province}')

    # Creating dim_district
    start_dim_district = datetime.now()
    print(f'Creating dim_district at {start_dim_district}')
    create_data.create_dim_table(conn_mysql,
                                cur_mysql,
                                conf_postgres,
                                '/root/final-project/sql_script/dim_district.sql',
                                'dim_district')
    end_dim_district = datetime.now()
    duration_dim_district = end_dim_province -start_dim_province
    print(f'Successfully created dim_district at {end_dim_district}. Duration: {duration_dim_district}')
    # Creating dim_case
    start_dim_case = datetime.now()
    print(f'Creating dim_case at {start_dim_case}')
    create_data.create_table(conn_postgres,
                            cur_postgres,
                            conf_postgres,
                            '/root/final-project/sql_script/dim_case.sql',
                            'dim_case')
    end_dim_case = datetime.now()
    duration_dim_case = end_dim_case - start_dim_case
    print(f'Successfully created dim_case at {end_dim_case}. Duration: {duration_dim_case}')

if __name__ == '__main__':
    main()


