import configparser
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.authentication_context import AuthenticationContext
from io import StringIO, BytesIO
import os
import glob
from dateutil.parser import parse
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to load configuration from a given path
def load_config(config_path):
    # Create a ConfigParser object
    config = configparser.ConfigParser()
    # Read the configuration file from the provided path
    config.read(config_path)
    # Log that the configuration was successfully loaded
    logger.info(f"Configuration loaded from {config_path}")
    # Return the loaded configuration
    return config

# Function to get a connection string based on the provided database configuration
def get_connection_string(db_config):
    # Check if driver connection string is provided
    if db_config.get('driver_conn', '') != '':
        try:
            # Construct the driver connection string
            conn_str = f"mssql+pyodbc://@{db_config['driver_conn']}"
            # Create an engine and test the connection
            engine = create_engine(conn_str)
            connection = engine.connect()
            connection.close()
            # Log success and return the connection string
            logger.info("Driver connection successful")
            return conn_str
        except Exception as e:
            # Log failure to use driver connection
            logger.error(f"Failed to use driver: {e}")

    # Check if Single Sign-On (SSO) is enabled
    if db_config.get('sso') == 'yes':
        try:
            # Construct the SSO connection string
            sso_conn_str = f"mssql+pyodbc://{db_config['server']}/{db_config['database']}?trusted_connection=yes&driver=SQL+Server"
            # Create an engine and test the connection
            engine = create_engine(sso_conn_str)
            connection = engine.connect()
            connection.close()
            # Log success and return the connection string
            logger.info("SSO connection successful")
            return sso_conn_str
        except Exception as e:
            # Log failure to use SSO connection
            logger.error(f"Failed to use SSO: {e}")

    # Fallback to basic connection with username and password
    try:
        # Construct the basic connection string
        fallback_conn_str = f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@{db_config['server']}/{db_config['database']}?driver=SQL+Server"
        # Create an engine and test the connection
        engine = create_engine(fallback_conn_str)
        connection = engine.connect()
        connection.close()
        # Log success and return the connection string
        logger.info("Basic connection successful")
        return fallback_conn_str
    except Exception as e:
        # Log failure to use basic connection and raise an exception
        logger.error(f"Failed to use basic connection: {e}")
        raise Exception("All connections failed")

# Function to fetch data from a SharePoint file
def fetch_data_from_sharepoint(site_url, file_url, username, password, file_format, sheet_name=None):
    # Authenticate with SharePoint
    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        # Create a client context using the authentication context
        ctx = ClientContext(site_url, ctx_auth)
    else:
        # Log the authentication error and raise an exception
        error_msg = ctx_auth.get_last_error()
        logger.error(f"Failed to authenticate: {error_msg}")
        raise Exception(error_msg)

    # Fetch the file content from SharePoint
    response = File.open_binary(ctx, file_url)
    
    # Parse the file content based on the file format
    if file_format.lower() == 'csv':
        # Read the content as CSV
        file_content = response.content.decode('utf-8')
        data_df = pd.read_csv(StringIO(file_content))
    elif file_format.lower() == 'xlsx':
        # Read the content as Excel
        file_content = BytesIO(response.content)
        data_df = pd.read_excel(file_content, sheet_name=sheet_name)
    else:
        # Log the unsupported file format error and raise an exception
        error_msg = f"Unsupported file format: {file_format}"
        logger.error(error_msg)
        raise Exception(error_msg)

    # Log success and return the data as a DataFrame and the context
    logger.info(f"Data fetched from SharePoint file {file_url} in {file_format} format")
    return data_df, ctx

# Function to fetch data from an SQL database using a query
def fetch_data_from_SQL(db_config, query):
    # Get the connection string
    conn_str = get_connection_string(db_config)
    # Create an engine using the connection string
    engine = create_engine(conn_str)
    # Execute the query and fetch data into a DataFrame
    df = pd.read_sql(query, engine)
    # Log success and return the DataFrame
    logger.info(f"Data fetched from SQL with query: {query}")
    return df

# Function to execute a SQL query
def execute_query(conn_str, query, params=None):
    try:
        # Create an engine using the connection string
        engine = create_engine(conn_str)
        # Connect to the database and execute the query
        with engine.connect() as connection:
            if params:
                # Execute the query with parameters if provided
                result = connection.execute(text(query), params)
            else:
                # Execute the query without parameters
                result = connection.execute(text(query))
        # Return the result of the query
        return result
    except Exception as e:
        # Log the error and raise an exception
        logger.error(f"An error occurred while executing the query: {e}", exc_info=True)
        raise

# Function to save data to SharePoint
def save_to_sharepoint(ctx, df, target_folder_url, file_name):
    # Convert DataFrame to Excel format
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    # Upload the file to the target SharePoint folder
    folder = ctx.web.get_folder_by_server_relative_url(target_folder_url)
    folder.upload_file(file_name, output)
    ctx.execute_query()
    # Log success
    logger.info(f"Data has been successfully saved to {target_folder_url}/{file_name} on SharePoint")

# Function to get the latest file in a directory matching a pattern
def get_latest_file(file_path, pattern):
    # Find all files matching the pattern in the specified path
    files = glob.glob(os.path.join(file_path, pattern))
    # Get the latest file based on the modification time
    latest_file = max(files, key=os.path.getmtime)
    # Log the latest file found and return its path
    logger.info(f"Latest file found: {latest_file}")
    return latest_file

# Function to parse a fuzzy date string and return a date object
def parse_date_full(date_fuz_str):
    # Check if the input is NaN
    if pd.isna(date_fuz_str):
        return np.nan
    else:
        # Parse the fuzzy date string
        date_tup = parse(date_fuz_str, fuzzy_with_tokens=True)
        datetime_val = date_tup[0]
        # Extract the date part and return it
        date_val = datetime_val.date()
        return date_val

# Function to backup a table in the database
def backup_table(conn_str, source_table, backup_table):
    try:
        # Construct the query to copy data from the source table to the backup table
        query = f"INSERT INTO {backup_table} SELECT * FROM {source_table}"
        # Execute the query
        execute_query(conn_str, query, {})
        # Log success
        logger.info(f"Backup of table {source_table} to {backup_table} completed successfully.")
    except Exception as e:
        # Log the error and raise an exception
        logger.error(f"An error occurred during backup: {e}", exc_info=True)
