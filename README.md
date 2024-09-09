# CodeAndDataPortfolio

This repository serves as a portfolio showcasing my work in data analysis. It includes a collection of scripts, modules, and projects that demonstrate my expertise in data-driven insights, statistical analysis, and automation. Each project is designed to highlight problem-solving techniques, and innovative solutions in real-world data scenarios.

This documentation helps in maintaining and understanding the usage of each function within the `functions_provider_utils.py` module.
Usage details are from the: `script_provider_supervisor_table_update.py`, `script_provider_oh_tins_export_sharepoint.py`, `script_provider_ciw_inquiry_update.py`, and `script_FHAS_NextGen.py`

### `functions_provider_utils.py` Module Documentation

---

### Function: `load_config`

**Description:** 
Loads configuration settings from a .ini file.

**Used by:**
- `script_provider_supervisor_table_update.py`
- `script_provider_oh_tins_export_sharepoint.py`
- `script_provider_ciw_inquiry_update.py`

---

### Function: `get_connection_string`

**Description:**
Gets the database connection string using SQLAlchemy format. Tries different methods (driver, SSO, basic connection) to establish a connection.

**Used by:**
- `provider_supervisor_table_update.py`
- `provider_ciw_inquiry_update.py`

---

### Function: `fetch_data_from_sharepoint`

**Description:**
Fetches data from SharePoint and returns it as a DataFrame.

**Used by:**
- `provider_supervisor_table_update.py`
- `provider_oh_tins_export_sharepoint.py`

---

### Function: `fetch_data_from_SQL`

**Description:**
Fetches data from a SQL database using a query and returns it as a DataFrame.

**Used by:**
- `provider_oh_tins_export_sharepoint.py`

---

### Function: `execute_query`

**Description:**
Executes a SQL query with optional parameters using SQLAlchemy.

**Used by:**
- `provider_supervisor_table_update.py`

---

### Function: `save_to_sharepoint`

**Description:**
Saves a DataFrame to a SharePoint folder.

**Used by:**
- `provider_oh_tins_export_sharepoint.py`

---

### Function: `get_latest_file`

**Description:**
Gets the latest file from a directory based on the pattern.

**Used by:**
- `provider_ciw_inquiry_update.py`

---

### Function: `parse_date_full`

**Description:**
Parses a fuzzy date string and returns the date part.

**Used by:**
- `provider_ciw_inquiry_update.py`

---

### Function: `backup_table`

**Description:**
Backs up a table by copying all its data to another table.

**Used by:**
- `provider_supervisor_table_update.py`

---

### Summary of Script Usage:

1. **script_provider_supervisor_table_update.py**
    - Functions used: `load_config`, `get_connection_string`, `fetch_data_from_sharepoint`, `execute_query`, `backup_table`

2. **script_provider_oh_tins_export_sharepoint.py**
    - Functions used: `load_config`, `fetch_data_from_sharepoint`, `fetch_data_from_SQL`, `save_to_sharepoint`

3. **script_provider_ciw_inquiry_update.py**
    - Functions used: `load_config`, `get_connection_string`, `get_latest_file`, `parse_date_full`

4. **script_FHAS_NextGen.py**
    - Functions used: 

