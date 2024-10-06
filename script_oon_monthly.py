# ----------------------------------------------------------------------------------------
# Script Name: script_oon_monthly.py
# Author: Andrew Correa 
# ----------------------------------------------------------------------------------------
# Script Overview:
# This script processes a CSV report, extracts the latest file from a specified folder, 
# and inserts the data into a SQL database using batch processing. It leverages configuration 
# details from an external file and uses utility functions from functions_provider_utils module for 
# handling configuration, database connections, and logging.
# ----------------------------------------------------------------------------------------
# Step 1: Load Configuration
# - The script begins by loading configuration settings from an INI file (config_provider_module.ini).
# - The settings include database connection details, folder paths, file extensions, and table names.
# - This step uses the `load_config` function from the functions_provider_utils module.

# Step 2: Establish Database Connection
# - The script generates a database connection string based on the configuration.
# - It uses the `get_connection_string` function to determine whether to use SSO, driver-based, or basic authentication.
# - The connection string is used to create an SQLAlchemy engine.

# Step 3: Get Latest File from Folder
# - The script searches a folder for the latest file matching a specified extension (e.g., CSV).
# - This is done using the `get_latest_file` function from the functions_provider_utils module, which selects the most recently modified file.

# Step 4: Read CSV Data
# - The script reads the contents of the latest CSV file into a pandas DataFrame.
# - All columns are treated as strings to maintain consistency in data type.
# - This step uses a custom `read_csv` function to handle file reading.

# Step 5: Data Validation and Logging
# - The script logs the data types of each column to provide insight into the structure of the file.
# - It also checks for missing values in the data and logs this information for review.

# Step 6: Insert Data into SQL Database
# - The script inserts the DataFrame into the specified SQL table using batch processing.
# - Data is inserted in batches of a configurable size (default is 50,000 rows per batch) to optimize performance.
# - The `insert_data_to_sql` function manages the batch insertion process.

# Step 7: Logging and Error Handling
# - The script logs progress at key stages, including the start and end of file processing, and any errors encountered.
# - If an error occurs at any point, the script captures the exception and logs the details before exiting.
# ----------------------------------------------------------------------------------------

import os
import pandas as pd
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from functions_provider_utils import (
    load_config, 
    get_connection_string, 
    get_latest_file, 
    logger
)

# ---------------------------------------------------------------------------------------- #
# Function: read_csv
# Description: Reads a CSV file from a given file path and forces all columns to be strings.
# Dependencies: pandas, logging
# Arguments:
# - file_path: The path to the CSV file.
# Returns: A pandas DataFrame with the CSV contents.
# ---------------------------------------------------------------------------------------- #
def read_csv(file_path):
    try:
        logger.info(f"Attempting to read CSV file: {file_path}")
        # Read CSV and force all columns to be treated as strings
        df = pd.read_csv(file_path, dtype=str)
        logger.info(f"Successfully read CSV file: {file_path} with {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {e}")
        raise

# ---------------------------------------------------------------------------------------- #
# Function: insert_data_to_sql
# Description: Inserts a pandas DataFrame into an SQL table in batches.
# Dependencies: sqlalchemy, logging
# Arguments:
# - data: A pandas DataFrame containing the data to be inserted.
# - table_name: The name of the table to insert the data into.
# - engine: SQLAlchemy engine object for connecting to the database.
# - batch_size: The number of rows to insert in each batch (default: 50,000).
# ---------------------------------------------------------------------------------------- #
def insert_data_to_sql(data, table_name, engine, batch_size=50000):
    try:
        logger.info(f"Starting batch data insertion into table: {table_name}.")
        start_time = time.time()
        
        columns = ', '.join([f'"{col}"' for col in data.columns])
        placeholders = ', '.join([f':{col}' for col in data.columns])
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        
        data_tuples = data.to_dict(orient='records')
        
        inserted_rows = 0
        with engine.connect() as connection:
            # Insert data in batches
            for batch_start in range(0, len(data_tuples), batch_size):
                batch_end = batch_start + batch_size
                batch = data_tuples[batch_start:batch_end]
                
                connection.execute(text(query), batch)
            
                inserted_rows += len(batch)
                logger.info(f"Inserted batch of {len(batch)} rows. Total inserted: {inserted_rows}/{len(data_tuples)}")
        
        logger.info(f"Successfully inserted {inserted_rows} rows into {table_name} in {time.time() - start_time:.2f} seconds.")
    except SQLAlchemyError as e:
        logger.error(f"Error inserting data into SQL: {e}")
        raise

# ---------------------------------------------------------------------------------------- #
# Function: import_report
# Description: Main function to orchestrate the process of reading a CSV file, processing it, and inserting data into the database.
# Dependencies: os, sqlalchemy, time, functions_provider_utils
# ---------------------------------------------------------------------------------------- #
def import_report():
    try:
        logger.info("Starting the import process.")
        start_time = time.time()
        
        # Load configuration
        config_path = 'config_provider_module.ini'
        logger.info(f"Loading configuration from {config_path}")
        config = load_config(config_path)
        
        db_config = {
            'user': config['DatabaseConfig']['username'],
            'password': config['DatabaseConfig']['password'],
            'server': config['DatabaseConfig']['server'],
            'database': config['DatabaseConfig']['database'],
            'sso': config['DatabaseConfig'].get('sso', 'Yes'),  
            'driver_conn': config['DatabaseConfig'].get('driver_conn', 'no')  
        }
        folder_path = config['OONConfig']['folder_path_monthly']
        file_extension = config['OONConfig']['file_extension_monthly']
        table_name = config['OONConfig']['table_name_monthly']

        # Get the database connection string
        conn_str = get_connection_string(db_config)
        engine = create_engine(conn_str)
        logger.info(f"Successfully connected to the database {db_config['database']}.")

        latest_file = get_latest_file(folder_path, file_extension)
        logger.info(f"Processing latest file: {latest_file}")
        file_start_time = time.time()
        
        data = read_csv(latest_file)  
        
        logger.info(f"Data types in the CSV for {latest_file}: \n{data.dtypes}")

        missing_values = data.isnull().sum()
        logger.info(f"Missing values per column for {latest_file}: \n{missing_values}")

        insert_data_to_sql(data, table_name, engine)

        logger.info(f"File {latest_file} processed and data inserted in {time.time() - file_start_time:.2f} seconds.")
        
        logger.info(f"All files processed. Total time: {time.time() - start_time:.2f} seconds.")
        
    except Exception as e:
        logger.error(f"Error encountered: {e}")
        print(f"Error encountered: {e}")

if __name__ == "__main__":
    import_report()
