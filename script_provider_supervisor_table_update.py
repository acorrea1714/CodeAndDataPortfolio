# Script Name: script_provider_supervisor_table_update.py
# Author: Andrew Correa 

# Purpose:
# This script updates a database table with data fetched from a SharePoint file.
# It either updates existing records or inserts new records based on the data.

# Requirements:
# - provider_utils module for utility functions
# - Config file 'config_provider_module.ini' for database and SharePoint configs
# - Logging for recording script activities and errors

# Steps:
# 1. Load config settings.
# 2. Save backup of current manager table.
# 3. Fetch data from SharePoint.
# 4. Establish database connection.
# 5. Fetch all current records' IDs from the database.
# 6. Iterate over the fetched data and update or insert values into the database.
# 7. Delete records from database where IDs not found in the CSV.
# 8. Log the results and any errors encountered.

from PROVIDER.functions_provider_utils import load_config, fetch_data_from_sharepoint, get_connection_string, execute_query, backup_table, logger

def update_table():
    try:
        # Load config settings from 'config_provider_module.ini'
        config_path = 'config_provider_module.ini'
        config = load_config(config_path)

        # Extract database and SharePoint configs
        db_config = config['DatabaseConfig']
        sharepoint_config = config['SharePointConfig']

        # Fetch data from SharePoint
        data_df, _ = fetch_data_from_sharepoint(
            site_url=sharepoint_config['ProviderAnalyticsFileExchange'],
            file_url=sharepoint_config['supervisor_list_path'],
            username=sharepoint_config['username'],
            password=sharepoint_config['password'],
            file_format='csv'
        )

        # Get the database connection string
        conn_str = get_connection_string(db_config)

        # Clear values from Backup table
        clear_query = "DELETE FROM [].[]"
        execute_query(conn_str, clear_query)
        logger.info("Cleared old records from backup table: [].[]")

        # Perform backup of prod table (source_table, backup_table)
        backup_table(conn_str, '[].[]', '[].[]')

        # Get the IDs from the CSV file
        csv_ids = set(data_df['US Domain ID'].tolist())

        # Process each row in the fetched data
        for index, row in data_df.iterrows():
            us_domain_id = row['US Domain ID']

            # Update existing record or insert a new one
            update_query = """
                UPDATE [].[]
                SET [Associate Name] = :associate_name,
                    [Supervisor Name] = :supervisor_name
                WHERE [US Domain ID] = :us_domain_id
            """
            result = execute_query(conn_str, update_query, {
                'us_domain_id': us_domain_id,
                'associate_name': row['Associate Name'],
                'supervisor_name': row['Supervisor Name']
            })

            if result.rowcount > 0:
                logger.info(f"Updated record for Associate: {us_domain_id} - {row['Associate Name']} to {row['Supervisor Name']}")
            else:
                insert_query = """
                    INSERT INTO [].[]
                    ([US Domain ID], [Associate Name], [Supervisor Name])
                    VALUES (:us_domain_id, :associate_name, :supervisor_name)
                """
                execute_query(conn_str, insert_query, {
                    'us_domain_id': us_domain_id,
                    'associate_name': row['Associate Name'],
                    'supervisor_name': row['Supervisor Name']
                })
                logger.info(f"Inserted new record for Associate: {us_domain_id} - {row['Associate Name']}")

        # Delete records not found in the CSV
        delete_query = """
            DELETE FROM [].[]
            WHERE [US Domain ID] NOT IN ({})
        """.format(','.join(f"'{id}'" for id in csv_ids))
        execute_query(conn_str, delete_query)
        logger.info(f"Deleted records from [].[] that were not found in the CSV file.")

        # Log the completion of script execution
        logger.info("Script execution complete.")

    except Exception as e:
        # Log any errors encountered during script execution
        logger.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    update_table()
