# ---------------------------------------------------------------------------------------- #
# Script Name: script_oon_tax.py
# Author: Andrew Correa
# ---------------------------------------------------------------------------------------- #
# Purpose:
# Automate the process of importing the latest CSV file from a specified folder into a SQL 
# database. The script:
# - Loads configuration settings (database, file paths).
# - Retrieves the latest CSV file.
# - Reads the file into a pandas DataFrame.
# - Logs data types and missing values.
# - Inserts the data into a SQL table in batches.
# ---------------------------------------------------------------------------------------- #
# Steps:
# 1. Initialize logging and start the import process.
# 2. Load configuration settings.
# 3. Set up and test the database connection.
# 4. Retrieve and read the latest CSV file.
# 5. Log data insights (types, missing values).
# 6. Insert data into the SQL table in batches.
# 7. Log completion and handle errors.
# ---------------------------------------------------------------------------------------- #
# Requirements:
# - provider_utils module (load_config, get_connection_string, get_latest_file, logger)
# - Config file 'config_provider_module.ini' for settings.
# ---------------------------------------------------------------------------------------- #

import os
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from functions_provider_utils import (
    load_config, 
    get_connection_string, 
    logger,
    get_latest_file
)

# ---------------------------------------------------------------------------------------- #
# Function: read_csv
# Description: Reads a CSV file and returns a DataFrame, forcing all columns to be strings.
# Dependencies: pandas, logging
# Arguments:
# - file_path: The path of the CSV file to read.
# Returns: A DataFrame with the content of the CSV file.
# ---------------------------------------------------------------------------------------- #
def read_csv(file_path):
    try:
        logger.info(f"Attempting to read CSV file: {file_path}")
        df = pd.read_csv(file_path, dtype=str)  # Force all columns to be treated as strings
        logger.info(f"Successfully read CSV file: {file_path} with {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {e}")
        raise

# ---------------------------------------------------------------------------------------- #
# Function: insert_data_to_sql
# Description: Inserts data from a DataFrame into a SQL table in batches.
# Dependencies: pandas, sqlalchemy, logging
# Arguments:
# - data: The DataFrame containing the data to insert.
# - table_name: The name of the SQL table to insert data into.
# - engine: The SQLAlchemy engine for database connection.
# - batch_size: Number of rows to insert per batch (default is 105).
# ---------------------------------------------------------------------------------------- #
def insert_data_to_sql(data, table_name, engine, batch_size=105):
    try:
        logger.info(f"Starting batch data insertion into table: {table_name}.")
        start_time = time.time()

        total_rows = len(data)
        inserted_rows = 0
        
        # Using pandas to_sql method for efficient batch insertion with a callback to track progress
        for batch_start in range(0, total_rows, batch_size):
            # Define the end of the batch slice
            batch_end = min(batch_start + batch_size, total_rows)
            batch_data = data.iloc[batch_start:batch_end]

            # Insert the batch
            batch_data.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

            # Log progress
            inserted_rows += len(batch_data)
            logger.info(f"Inserted batch of {len(batch_data)} rows. Total inserted: {inserted_rows}/{total_rows}")

        logger.info(f"Successfully inserted {inserted_rows} rows into {table_name} in {time.time() - start_time:.2f} seconds.")
    except SQLAlchemyError as e:
        logger.error(f"Error inserting data into SQL: {e}")
        raise

# ---------------------------------------------------------------------------------------- #
# Function: import_report
# Description: Main function to orchestrate the report import process from CSV to SQL.
# Dependencies: logging, time, pandas, sqlalchemy, os
# ---------------------------------------------------------------------------------------- #
def import_report():
    try:
        logger.info("Starting the import process.")
        start_time = time.time()
        
        # Load configuration
        config_path = 'config_provider_module.ini'
        logger.info(f"Loading configuration from {config_path}")
        config = load_config(config_path)
        
        # Database configuration
        db_config = {
            'user': config['DatabaseConfig']['username'],
            'password': config['DatabaseConfig']['password'],
            'server': config['DatabaseConfig']['server'],
            'database': config['DatabaseConfig']['database'],
            'sso': config['DatabaseConfig'].get('sso', 'Yes'),  
            'driver_conn': config['DatabaseConfig'].get('driver_conn', 'no')  
        }

        # File and table configurations
        folder_path = config['OONConfig']['folder_path']
        file_extension = config['OONConfig']['file_extension']
        table_name = config['OONConfig']['table_name']

        # Get the connection string using utility function
        conn_str = get_connection_string(db_config)
        engine = create_engine(conn_str)
        logger.info(f"Successfully connected to the database {db_config['database']}.")

        # Get the latest file instead of all files
        latest_file = get_latest_file(folder_path, f"*{file_extension}")
        logger.info(f"Processing latest file: {latest_file}")
        file_start_time = time.time()

        # Read CSV data
        data = read_csv(latest_file)

        # Log data types and missing values for the CSV
        logger.info(f"Data types in the CSV for {latest_file}: \n{data.dtypes}")
        missing_values = data.isnull().sum()
        logger.info(f"Missing values per column for {latest_file}: \n{missing_values}")

        # Insert data into SQL database
        insert_data_to_sql(data, table_name, engine)

        logger.info(f"File {latest_file} processed and data inserted in {time.time() - file_start_time:.2f} seconds.")
        
        logger.info(f"Import process completed. Total time: {time.time() - start_time:.2f} seconds.")
        
    except Exception as e:
        logger.error(f"Error encountered: {e}")
        print(f"Error encountered: {e}")

if __name__ == "__main__":
    import_report()
