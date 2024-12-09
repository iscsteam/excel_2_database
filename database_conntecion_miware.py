import pandas  as pd
import psycopg2

Username="iscs_ats"
Host="fleet-fish-5790.7s5.aws-ap-south-1.cockroachlabs.cloud"
Database="ats_iscs"
Port="26257"
password = "w2mrGcYWJLvxDfXgAhAZ1Q"
database_url = f"postgresql://{Username}:{password}@{Host}:{Port}/{Database}"

def connect_to_postgresql(database_url):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(database_url)
        print("The database is connected")
        return conn
    except Exception as e:
        print("Error connecting to PostgreSQL:", e)
        return None

connection = connect_to_postgresql(database_url)
