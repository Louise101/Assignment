import pandas as pd
from sqlalchemy import create_engine



# MySQL connection
mysql_engine = create_engine("mysql+mysqlconnector://root:T3st_pA$$@localhost/HF_data")

# PostgreSQL connection
postgres_engine = create_engine("postgresql+psycopg2://louisefinlayson:test_pass@localhost:5432/HF_OLAP2")

# Extract data from MySQL
def extract_data():
    queries = {
        'pat_info': "SELECT * FROM pat_info",
        'gp_prac': "SELECT * FROM gp_prac",
        'tec_no_key': "SELECT * FROM tec_no_key"
    }
    data = {table: pd.read_sql_query(query, mysql_engine) for table, query in queries.items()}
    return data

#Transform data into OLAP schema
def transform_data(data):
    # Create fact table named fact_pat_gp
    fact_pat_gp = pd.merge(
        data['pat_info'],
        data['gp_prac'],
        how='inner',
        left_on='Registered_GP_Practice_key',
        right_on='Registered_GP_Practice_key'
    )[[
        'Patient_ID', 'Registered_GP_Practice_key'
    ]]


    # Create dimenstion table named Dim_GP_Practice
    dim_gp_practice = data['gp_prac'][[
        'Registered_GP_Practice_key', 'Registered_GP_Practice', 'GPS_Coordinates_lat',
        'GPS_Coordinates_long', 'GP_Population_2024', 'GP_area_deprevity_score_overall',
        'Deprevity_catagory_overall', 'GP_area_deprivity_rank_health', 'Deprevity_catagory_health','GP_area_deprivity_rank_access_to_services','Deprevity_catagory_access' 
    ]]

    # Create dimention table named Dim_Patient
    dim_patient = pd.merge(
        data['pat_info'],
        data['tec_no_key'],
        how='inner',
        left_on = 'TEC_or_No_Key',
        right_on = 'TEC_or_No_Key')[['Patient_ID', 'Tec_or_No']]

    return {
        'fact_pat_gp': fact_pat_gp,
        'dim_gp_practice': dim_gp_practice,
        'dim_patient': dim_patient,
    }

#Load data into PostgreSQL
def load_data(transformed_data):
    for table_name, df in transformed_data.items():
        df.to_sql(table_name, postgres_engine, if_exists='replace', index=False)
        print(f"Loaded {table_name} into PostgreSQL.")

# run
    print("Extracting data from MySQL")
    oltp_data = extract_data()

    print("Transforming data into OLAP")
    olap_data = transform_data(oltp_data)

    print("Loading data into PostgreSQL")
    load_data(olap_data)

    print("ETL process completed successfully.")