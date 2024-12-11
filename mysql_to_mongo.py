import pymysql
from pymongo import MongoClient
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MONGO_URI, MONGO_DB

#MySQL connection
def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

#MongoDB  connection 
def get_mongo_client(): 
    return MongoClient(MONGO_URI)[MONGO_DB]

# Extract data from MySQL
def extract_data():
    conn = get_mysql_connection() 
    cursor = conn.cursor(pymysql.cursors.DictCursor)# fetches resutls as a python dictionary to make the data more human readable

#SQL query to join tables and select the relevant data
    query = """ 
    SELECT 
        p.Patient_ID,  
        p.Registered_GP_Practice_key, 
        p.TEC_or_No_Key, 
        g.Registered_gp_Practice_key,
        g.Registered_GP_Practice, 
        g.GPS_Coordinates_lat,
        g.GPS_Coordinates_long, 
        g.GP_Population_2024,
        g.GP_area_deprevity_score_overall,
        g.Deprevity_catagory_overall,
        g.GP_area_deprivity_rank_health,
        g.Deprevity_catagory_health,
        g.GP_area_deprivity_rank_access_to_services,
        g.Deprevity_catagory_access,
        t.Tec_or_No,
        t.TEC_or_No_Key

    FROM pat_info p
    JOIN gp_prac g ON g.Registered_GP_Practice_key = p.Registered_GP_Practice_key
    JOIN tec_no_key t ON t.TEC_or_No_Key = p.TEC_or_No_Key
    """

    
    cursor.execute(query)
    data = cursor.fetchall() # fetches the rows of data retuned by the query

    conn.close()
    return data

# Transform data 
def transform_data(raw_data): 
    patients = {}
    for record in raw_data:
        patient_id = record["Patient_ID"]
        if patient_id not in patients:
            patients[patient_id] = {
                "patient_id": patient_id,
                "gp_practice": {
                    "practice_name": record["Registered_GP_Practice"],
                    "GPS location": record["GPS_Coordinates_lat"],
                    "GPS location longitide": record["GPS_Coordinates_long"],
                    "Practice population": record["GP_Population_2024"],
                    "Overall depravity rank": record["GP_area_deprevity_score_overall"],
                    "Overall depravity catagory": record["Deprevity_catagory_overall"],
                    "Health depravity rank": record["GP_area_deprivity_rank_health"],
                    "Health depravity catagory": record["Deprevity_catagory_health"],
                    "Access to services depravity rank": record["GP_area_deprivity_rank_access_to_services"],
                    "Access to services depravity catagory": record["Deprevity_catagory_access"],
                },
                "TEC user?": record["Tec_or_No"],
                
            }
      
    return list(patients.values()) #converts result into a list of values for transferring to mongoDB

# Load transformed data into MongoDB
def load_data(transformed_data):
    mongo_client = get_mongo_client() 
    collection = mongo_client["HF"] 
    collection.insert_many(transformed_data) 

#executes funtions

print("Extracting data...")
raw_data = extract_data()
print(f"Extracted {len(raw_data)} records.")

print("Transforming data...")
transformed_data = transform_data(raw_data)
print(f"Transformed into {len(transformed_data)} patient records.")

print("Loading data into MongoDB...")
load_data(transformed_data)
print("ETL process completed successfully.")
