from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import pandas as pd
import logging
from psycopg2.extras import execute_values
from psycopg2 import connect

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

# Replace with your actual username and password, or fetch from a database or environment variables.
VALID_USERNAME = "your_username"
VALID_PASSWORD = "your_password"

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    """Validate username and password."""
    if credentials.username != VALID_USERNAME or credentials.password != VALID_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="app.log",  # Save logs to a file
    filemode="w"         # Overwrite the file each time (use "a" for appending)
)

logger = logging.getLogger(__name__)

app = FastAPI(debug=True)


# Pydantic Models
class DatabaseParams(BaseModel):
    username: str
    password: str
    host: str
    port: int
    database: str
    table_name: str
    batch_percentage: int = 30

class ProcessDataRequest(BaseModel):
    filename: str  # Add filename as input
    db_params: DatabaseParams

# Functions

def connect_to_postgresql(db_params: DatabaseParams):
    """Establish a connection to PostgreSQL or CockroachDB."""
    try:
        # Correctly format the connection string for CockroachDB
        conn_str = f"postgresql://{db_params.username}:{db_params.password}@{db_params.host}:{db_params.port}/{db_params.database}"
        conn = connect(conn_str)
        logger.info("Connected to database successfully")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise


def create_table_if_not_exists(conn, df: pd.DataFrame, table_name: str) -> None:
    """Create table if it doesn't exist using pure SQL."""
    columns = [f"{col} TEXT" for col in df.columns]
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns)}
    )
    """
    with conn.cursor() as cur:
        try:
            cur.execute(create_table_sql)
            conn.commit()
            logger.info(f"Table {table_name} is ready")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating table: {str(e)}")
            raise

def insert_data_in_batches(conn, df: pd.DataFrame, table_name: str, batch_percentage: int) -> None:
    """Insert data using psycopg2 execute_values in percentage-based batches."""
    df = df.astype(str)  # Convert all data to strings
    total_rows = len(df)
    batch_size = int(total_rows * (batch_percentage / 100))
    num_batches = (total_rows + batch_size - 1) // batch_size  # Round up division

    logger.info(f"Total rows: {total_rows}")
    logger.info(f"Batch size ({batch_percentage}%): {batch_size} rows")
    logger.info(f"Number of batches: {num_batches}")

    columns = df.columns.tolist()
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

    with conn.cursor() as cur:
        for batch_num in range(num_batches):
            try:
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                batch_values = [tuple(x) for x in batch_df.values]
                execute_values(cur, insert_sql, batch_values)
                conn.commit()
                logger.info(f"Batch {batch_num + 1}/{num_batches} inserted successfully ({start_idx} to {end_idx} rows)")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error inserting batch {batch_num + 1}: {str(e)}")
                raise
     # Return necessary details
    return total_rows, batch_size, num_batches

def read_data(filename: str) -> pd.DataFrame:
    """Read data from CSV or Excel files."""
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File not found: {filename}")
    try:
        if filename.endswith(".csv"):
            data = pd.read_csv(filename)
            logger.info(f"Reading CSV file: {filename}")
        elif filename.endswith(".xlsx"):
            data = pd.read_excel(filename)
            logger.info(f"Reading Excel file: {filename}")
        elif filename.endswith(".xls"):
            data = pd.read_excel(filename, engine="xlrd")
            logger.info(f"Reading legacy Excel file: {filename}")
        else:
            raise ValueError("Unsupported file format!")
        return data
    except Exception as e:
        logger.error(f"Error reading file {filename}: {str(e)}")
        raise

def process_file_to_postgres(filename: str, db_params: DatabaseParams) -> tuple:
    """Process file and insert data into PostgreSQL."""
    # Connect to the database - pass the entire db_params object
    conn = connect_to_postgresql(db_params)  # Changed from db_params.database to db_params
    
    try:
        # Read the data from the file
        df = read_data(filename)
        
        # Create table if it doesn't exist
        create_table_if_not_exists(conn, df, db_params.table_name)  # Add this line
        
        # Insert data in batches and get the details
        total_rows, batch_size, num_batches = insert_data_in_batches(conn, df, db_params.table_name, db_params.batch_percentage)
        
        # Return the values to be used by the FastAPI response
        return total_rows, batch_size, num_batches
    except Exception as e:
        logger.error(f"Error in processing file {filename}: {str(e)}")
        raise
    finally:
        conn.close()



# #FastAPI Endpoint
# @app.post("/process-file")
# async def process_file(request: ProcessDataRequest):
#     try:
#         filename = request.filename  # Get filename from request
#         db_params = request.db_params  # Get db_params from request
        
#         # Process file and get details (total_rows, batch_size, num_batches)
#         total_rows, batch_size, num_batches = process_file_to_postgres(filename, db_params)
        
#         # Return the result in a structured response
#         return {
#             "message": "File processed and data inserted successfully",
#             "details": {
#                 "table_name": db_params.table_name,  # Access as object property
#                 "total_rows": total_rows,
#                 "batch_size": batch_size,
#                 "num_batches": num_batches
#             }
#         }
#     except Exception as e:
#         logger.error(f"Failed to process file: {e}")
#         raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")


# FastAPI Endpoint with authentication
@app.post("/process-file")
async def process_file(request: ProcessDataRequest, username: str = Depends(authenticate)):
    try:
        filename = request.filename  # Get filename from request
        db_params = request.db_params  # Get db_params from request
        
        # Process file and get details (total_rows, batch_size, num_batches)
        total_rows, batch_size, num_batches = process_file_to_postgres(filename, db_params)
        
        # Return the result in a structured response
        return {
            "message": "File processed and data inserted successfully",
            "details": {
                "table_name": db_params.table_name,
                "total_rows": total_rows,
                "batch_size": batch_size,
                "num_batches": num_batches
            }
        }
    except Exception as e:
        logger.error(f"Failed to process file: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

