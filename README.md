# `functions_provider_utils.py` Module Documentation

This documentation helps in maintaining and understanding the usage of each function within the `functions_provider_utils.py` module.  
Usage details are from the following scripts:  
- `script_provider_supervisor_table_update.py`
- `script_provider_oh_tins_export_sharepoint.py`
- `script_provider_ciw_inquiry_update.py`
- `script_FHAS_NextGen.py`
- `script_oon_monthly.py`
- `script_oon_tax.py`

---

### Function: `load_config`

**Description:**  
Loads the configuration file from the specified path and returns the parsed configuration object.

**Arguments:**
- `config_path` (str): The file path of the configuration file.

**Returns:**
- `ConfigParser`: A ConfigParser object containing the loaded configuration.

**Dependencies:**
- `configparser`
- `logging`

**Used by:**
- `script_provider_supervisor_table_update.py`
- `script_provider_oh_tins_export_sharepoint.py`
- `script_provider_ciw_inquiry_update.py`
- `script_oon_monthly.py`
- `script_oon_tax.py`

---

### Function: `get_basic_connection_str`

**Description:**  
Constructs and returns a basic authentication connection string for SQL Server.

**Arguments:**
- `db_config` (dict): A dictionary containing database connection details like user, password, server, and database.

**Returns:**
- `str`: A connection string for basic authentication.

**Dependencies:**
- `sqlalchemy`
- `logging`

---

### Function: `get_connection_string`

**Description:**  
Generates and tests a connection string using either driver connection, SSO, or basic authentication.

**Arguments:**
- `db_config` (dict): A dictionary containing database connection details and flags for driver or SSO connections.

**Returns:**
- `str`: A valid connection string based on the provided configuration.

**Dependencies:**
- `sqlalchemy`
- `logging`

**Used by:**
- `provider_supervisor_table_update.py`
- `provider_ciw_inquiry_update.py`
- `script_oon_monthly.py`
- `script_oon_tax.py`

---

### Function: `fetch_data_from_sharepoint`

**Description:**  
Authenticates to SharePoint, fetches a file (CSV or Excel), and loads the data into a DataFrame.

**Arguments:**
- `site_url` (str): SharePoint site URL.
- `file_url` (str): URL to the specific file on SharePoint.
- `username` (str): SharePoint username.
- `password` (str): SharePoint password.
- `file_format` (str): Format of the file to fetch (`csv` or `xlsx`).
- `sheet_name` (str, optional): Sheet name for Excel files.

**Returns:**
- `tuple`: A tuple containing:
  - `pd.DataFrame`: The data in the file as a DataFrame.
  - `ClientContext`: The SharePoint context object.

**Dependencies:**
- `office365`
- `pandas`
- `logging`

**Used by:**
- `provider_supervisor_table_update.py`
- `provider_oh_tins_export_sharepoint.py`

---

### Function: `fetch_data_from_SQL`

**Description:**  
Fetches data from SQL by executing the provided query and returns the result as a DataFrame.

**Arguments:**
- `db_config` (dict): Database configuration dictionary.
- `query` (str): SQL query to execute.
- `method` (int): Integer (1 or 2) representing the connection method (1: Driver, 2: Basic Authentication).

**Returns:**
- `pd.DataFrame` or `None`: A DataFrame containing the query results, or None if no rows are returned.

**Dependencies:**
- `sqlalchemy`
- `pandas`
- `os`
- `logging`

**Used by:**
- `provider_oh_tins_export_sharepoint.py`

---

### Function: `execute_query`

**Description:**  
Executes a SQL query against the database and returns the result.

**Arguments:**
- `conn_str` (str): SQLAlchemy connection string.
- `query` (str): SQL query to execute.
- `params` (dict, optional): Parameters for the query.

**Returns:**
- `ResultProxy`: The result of the executed query.

**Dependencies:**
- `sqlalchemy`
- `logging`

**Used by:**
- `provider_supervisor_table_update.py`

---

### Function: `save_to_sharepoint`

**Description:**  
Saves a DataFrame as an Excel file to a specified SharePoint folder.

**Arguments:**
- `ctx` (ClientContext): ClientContext object for SharePoint authentication.
- `df` (pd.DataFrame): DataFrame to save.
- `target_folder_url` (str): The URL of the SharePoint folder where the file will be saved.
- `file_name` (str): Name of the file to save.

**Dependencies:**
- `office365`
- `pandas`
- `logging`

**Used by:**
- `provider_oh_tins_export_sharepoint.py`

---

### Function: `get_latest_file`

**Description:**  
Retrieves the latest file matching a pattern in a specified directory.

**Arguments:**
- `file_path` (str): The directory path to search for files.
- `pattern` (str): The pattern to match files.

**Returns:**
- `str`: The path of the latest file.

**Dependencies:**
- `glob`
- `os`
- `logging`

**Used by:**
- `provider_ciw_inquiry_update.py`
- `script_oon_monthly.py`
- `script_oon_tax.py`

---

### Function: `parse_date_full`

**Description:**  
Parses a fuzzy date string and returns a date object.

**Arguments:**
- `date_fuz_str` (str): A string representing a fuzzy date.

**Returns:**
- `datetime.date` or `np.nan`: A date object if parsing is successful, otherwise NaN.

**Dependencies:**
- `dateutil.parser`
- `pandas`
- `numpy`

**Used by:**
- `provider_ciw_inquiry_update.py`

---

### Function: `backup_table`

**Description:**  
Backs up a table by copying its data into a backup table in the database.

**Arguments:**
- `conn_str` (str): SQLAlchemy connection string.
- `source_table` (str): The name of the table to back up.
- `backup_table` (str): The name of the backup table.

**Dependencies:**
- `sqlalchemy`
- `logging`

**Used by:**
- `provider_supervisor_table_update.py`

---

### Function: `log_deleted_records`

**Description:**  
Logs and fetches records that are about to be deleted from a database.

**Arguments:**
- `conn_str` (str): SQLAlchemy connection string.
- `delete_log_query` (str): SQL query to log the records about to be deleted.

**Returns:**
- `pd.DataFrame`: A DataFrame containing the records to be deleted.

**Dependencies:**
- `sqlalchemy`
- `pandas`
- `logging`

---

## Summary of Script Usage:

1. **script_provider_supervisor_table_update.py**
    - Functions used: `load_config`, `get_connection_string`, `fetch_data_from_sharepoint`, `execute_query`, `backup_table`

2. **script_provider_oh_tins_export_sharepoint.py**
    - Functions used: `load_config`, `fetch_data_from_sharepoint`, `fetch_data_from_SQL`, `save_to_sharepoint`

3. **script_provider_ciw_inquiry_update.py**
    - Functions used: `load_config`, `get_connection_string`, `get_latest_file`, `parse_date_full`

4. **script_FHAS_NextGen.py**
    - Functions used: `load_config`, `get_connection_string`, `fetch_data_from_sharepoint`, `get_latest_file`

5. **script_oon_monthly.py**
    - Functions used: `load_config`, `get_connection_string`, `get_latest_file`

6. **script_oon_tax.py**
    - Functions used: `load_config`, `get_connection_string`, `get_latest_file`

---
