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
from datetime import datetime

# Initialize logger
logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------- Function Definitions ----------------------------------- #

# ---------------------------------------------------------------------------------------- #
# Function: load_config
# Description: Loads the configuration file from the specified path and returns the parsed configuration object.
# Dependencies: configparser, logging
# Arguments:
# - config_path: The file path of the configuration file.
# Returns: A ConfigParser object containing the loaded configuration.
# ---------------------------------------------------------------------------------------- #
def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    logger.info(f"Configuration loaded from {config_path}")
    return config

# ---------------------------------------------------------------------------------------- #
# Function: get_basic_connection_str
# Description: Constructs and returns a basic authentication connection string for SQL Server.
# Dependencies: sqlalchemy, logging
# Arguments:
# - db_config: A dictionary containing database connection details like user, password, server, and database.
# Returns: A connection string for basic authentication.
# ---------------------------------------------------------------------------------------- #
def get_basic_connection_str(db_config):
    basic_auth_conn_str = f"mssql+pyodbc://{db_config['user']}:{db_config['password']}@{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    logger.info("Basic authentication connection string generated successfully")
    return basic_auth_conn_str

# ---------------------------------------------------------------------------------------- #
# Function: get_connection_string
# Description: Generates and tests a connection string using either driver connection, SSO, or basic authentication.
# Dependencies: sqlalchemy, logging
# Arguments:
# - db_config: A dictionary containing database connection details and flags for driver or SSO connections.
# Returns: A valid connection string based on the provided configuration.
# ---------------------------------------------------------------------------------------- #
def get_connection_string(db_config):
    if db_config.get('driver_conn', '').lower() != 'no':
        try:
            conn_str = f"mssql+pyodbc://@{db_config['driver_conn']}"
            engine = create_engine(conn_str)
            connection = engine.connect()
            connection.close()
            logger.info("Driver connection successful")
            return conn_str
        except Exception as e:
            logger.error(f"Failed to use driver: {e}")

    if db_config.get('sso', '').lower() == 'yes':
        try:
            sso_conn_str = f"mssql+pyodbc://{db_config['server']}/{db_config['database']}?trusted_connection=yes&driver=SQL+Server"
            engine = create_engine(sso_conn_str)
            connection = engine.connect()
            connection.close()
            logger.info("SSO connection successful")
            return sso_conn_str
        except Exception as e:
            logger.error(f"Failed to use SSO: {e}")

    if db_config.get('sso', '').lower() == 'no':
        try:
            basic_auth_conn_str = f"mssql+pyodbc://{db_config['user']}:{db_config['password']}@{db_config['server']}/{db_config['database']}?driver=SQL+Server"
            engine = create_engine(basic_auth_conn_str)
            connection = engine.connect()
            connection.close()
            logger.info("Basic authentication connection successful")
            return basic_auth_conn_str
        except Exception as e:
            logger.error(f"Failed to use basic authentication: {e}")
    
    raise ValueError("No valid connection method could be established.")

# ---------------------------------------------------------------------------------------- #
# Function: fetch_data_from_sharepoint
# Description: Authenticates to SharePoint, fetches a file (CSV or Excel), and loads the data into a DataFrame.
# Dependencies: office365, pandas, logging
# Arguments:
# - site_url: SharePoint site URL
# - file_url: URL to the specific file on SharePoint
# - username: SharePoint username
# - password: SharePoint password
# - file_format: Format of the file to fetch ('csv' or 'xlsx')
# - sheet_name: (Optional) Sheet name for Excel files
# Returns: A DataFrame containing the file's data.
# ---------------------------------------------------------------------------------------- #
def fetch_data_from_sharepoint(site_url, file_url, username, password, file_format, sheet_name=None):
    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)
    else:
        error_msg = ctx_auth.get_last_error()
        logger.error(f"Failed to authenticate: {error_msg}")
        raise Exception(error_msg)

    response = File.open_binary(ctx, file_url)
    
    if file_format.lower() == 'csv':
        file_content = response.content.decode('utf-8')
        data_df = pd.read_csv(StringIO(file_content))
    elif file_format.lower() == 'xlsx':
        file_content = BytesIO(response.content)
        data_df = pd.read_excel(file_content, sheet_name=sheet_name)
    else:
        error_msg = f"Unsupported file format: {file_format}"
        logger.error(error_msg)
        raise Exception(error_msg)

    logger.info(f"Data fetched from SharePoint file {file_url} in {file_format} format")
    return data_df, ctx

# ---------------------------------------------------------------------------------------- #
# Function: fetch_data_from_SQL
# Description: Fetches data from SQL by executing the provided query and returns the result as a DataFrame.
# Dependencies: sqlalchemy, pandas, os, logging
# Arguments:
# - db_config: Database configuration dictionary.
# - query: SQL query to execute.
# - method: Integer (1 or 2) representing the connection method (1: Driver, 2: Basic Authentication).
# Returns: A DataFrame containing the query results, or None if no rows are returned.
# ---------------------------------------------------------------------------------------- #
def fetch_data_from_SQL(db_config, query, method):
    def get_download_folder():
        return os.path.join(os.path.expanduser("~"), "Downloads")
    
    def save_query_to_file(query):
        download_folder = get_download_folder()
        file_path = os.path.join(download_folder, "executed_query.sql")
        with open(file_path, "w") as file:
            file.write(query)
        logger.info(f"Query saved to {file_path}")

    if method == 1:
        conn_str = get_connection_string(db_config)
    elif method == 2:
        conn_str = get_basic_connection_str(db_config)
    else:
        raise ValueError("Invalid method value. Please use 1 or 2.")

    logger.info(f"Connection string: {conn_str}")

    engine = create_engine(conn_str)

    try:
        save_query_to_file(query)

        with engine.connect() as connection:
            result = connection.execute(query)

            if result.returns_rows:
                df = pd.read_sql(query, connection)
                logger.info(f"Data fetched successfully. Rows returned: {len(df)}")
                return df
            else:
                logger.warning("Query executed successfully but returned no rows.")
                return None

    except Exception as e:
        logger.error(f"Error fetching data from SQL: {e}")
        return None

# ---------------------------------------------------------------------------------------- #
# Function: execute_query
# Description: Executes a SQL query against the database and returns the result.
# Dependencies: sqlalchemy, logging
# Arguments:
# - conn_str: SQLAlchemy connection string.
# - query: SQL query to execute.
# - params: (Optional) Parameters for the query.
# Returns: The result of the executed query.
# ---------------------------------------------------------------------------------------- #
def execute_query(conn_str, query, params=None):
    try:
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            if params:
                result = connection.execute(text(query), params)
            else:
                result = connection.execute(text(query))
        return result
    except Exception as e:
        logger.error(f"An error occurred while executing the query: {e}", exc_info=True)
        raise

# ---------------------------------------------------------------------------------------- #
# Function: save_to_sharepoint
# Description: Saves a DataFrame as an Excel file to a specified SharePoint folder.
# Dependencies: office365, pandas, logging
# Arguments:
# - ctx: ClientContext object for SharePoint authentication.
# - df: DataFrame to save.
# - target_folder_url: The URL of the SharePoint folder where the file will be saved.
# - file_name: Name of the file to save.
# ---------------------------------------------------------------------------------------- #
def save_to_sharepoint(ctx, df, target_folder_url, file_name):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    folder = ctx.web.get_folder_by_server_relative_url(target_folder_url)
    folder.upload_file(file_name, output)
    ctx.execute_query()
    logger.info(f"Data has been successfully saved to {target_folder_url}/{file_name} on SharePoint")

# ---------------------------------------------------------------------------------------- #
# Function: get_latest_file
# Description: Retrieves the latest file matching a pattern in a specified directory.
# Dependencies: glob, os, logging
# Arguments:
# - file_path: The directory path to search for files.
# - pattern: The pattern to match files.
# Returns: The path of the latest file.
# ---------------------------------------------------------------------------------------- #
def get_latest_file(file_path, pattern):
    files = glob.glob(os.path.join(file_path, pattern))
    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Latest file found: {latest_file}")
    return latest_file

# ---------------------------------------------------------------------------------------- #
# Function: parse_date_full
# Description: Parses a fuzzy date string and returns a date object.
# Dependencies: dateutil.parser, pandas, numpy
# Arguments:
# - date_fuz_str: A string representing a fuzzy date.
# Returns: A date object if parsing is successful, otherwise NaN.
# ---------------------------------------------------------------------------------------- #
def parse_date_full(date_fuz_str):
    if pd.isna(date_fuz_str):
        return np.nan
    else:
        date_tup = parse(date_fuz_str, fuzzy_with_tokens=True)
        datetime_val = date_tup[0]
        date_val = datetime_val.date()
        return date_val

# ---------------------------------------------------------------------------------------- #
# Function: backup_table
# Description: Backs up a table by copying its data into a backup table in the database.
# Dependencies: sqlalchemy, logging
# Arguments:
# - conn_str: SQLAlchemy connection string.
# - source_table: The name of the table to back up.
# - backup_table: The name of the backup table.
# ---------------------------------------------------------------------------------------- #
def backup_table(conn_str, source_table, backup_table):
    try:
        query = f"INSERT INTO {backup_table} SELECT * FROM {source_table}"
        execute_query(conn_str, query, {})
        logger.info(f"Backup of table {source_table} to {backup_table} completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during backup: {e}", exc_info=True)

# ---------------------------------------------------------------------------------------- #
# Function: load_sql_query
# Description: Loads the contents of a SQL file from the provided path.
# Dependencies: os
# Arguments:
# - file_path: The file path of the SQL file.
# Returns: The SQL query as a string.
# ---------------------------------------------------------------------------------------- #
def load_sql_query(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# ---------------------------------------------------------------------------------------- #
# Function: get_latest_file_from_sharepoint
# Description: Retrieves the latest file from a specified SharePoint folder based on the last modified or created time.
# Dependencies: office365, datetime, logging
# Arguments:
# - ctx: ClientContext object for SharePoint authentication.
# - folder_url: The URL of the SharePoint folder to search.
# Returns: The server-relative URL of the latest file.
# ---------------------------------------------------------------------------------------- #
def get_latest_file_from_sharepoint(ctx, folder_url):
    folder = ctx.web.get_folder_by_server_relative_url(folder_url).expand(["Files"]).get().execute_query()
    files = folder.files
    if not files:
        raise Exception("No files found in the SharePoint folder.")

    def get_file_time(file):
        modified_time = getattr(file, 'time_last_modified', None)
        if modified_time is None:
            return file.time_created
        if isinstance(modified_time, datetime):
            return modified_time
        else:
            try:
                return datetime.strptime(modified_time, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                logger.warning(f"Failed to parse time_last_modified for file: {file.name}, falling back to created_time")
                return file.time_created

    try:
        latest_file = max(files, key=get_file_time)
        logger.info(f"Latest file found: {latest_file.name}")
        return latest_file.serverRelativeUrl
    except Exception as e:
        logger.error(f"Failed to get latest file: {e}")
        raise

# ---------------------------------------------------------------------------------------- #
# Function: log_deleted_records
# Description: Logs and fetches records that are about to be deleted from a database.
# Dependencies: sqlalchemy, pandas, logging
# Arguments:
# - conn_str: SQLAlchemy connection string.
# - delete_log_query: SQL query to log the records about to be deleted.
# Returns: A DataFrame containing the records to be deleted.
# ---------------------------------------------------------------------------------------- #
def log_deleted_records(conn_str, delete_log_query):
    try:
        df = pd.read_sql(delete_log_query, conn_str)

        if df.empty:
            logger.info("No records to delete.")
            return df

        for index, row in df.iterrows():
            logger.info(f"Deleting record: US Domain ID: {row['US Domain ID']}, "
                        f"Associate Name: {row['Associate Name']}, Supervisor Name: {row['Supervisor Name']}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error fetching records to be deleted: {e}", exc_info=True)
        raise
