# Script Name: script_provider_oh_tins_export_sharepoint.py
# Author: Andrew Correa

# Purpose:
#     This script exports a report from a SQL database to SharePoint based on a list of provider TINs
#     retrieved from a SharePoint file. The steps involve loading Config settings, fetching data
#     from SharePoint, querying the SQL database, and saving the results back to SharePoint.

# Requirements:
#     - A Config file named 'config_provider_module.ini' with the necessary database and SharePoint settings.
#     - The provider_utils.py utility module containing helper functions for Config loading, 
#       data fetching from SharePoint and SQL, and saving data to SharePoint.

# Steps:
#     1. Load Config settings from 'config_provider_module.ini'.
#     2. Fetch the list of provider TINs from SharePoint.
#     3. Query the SQL database using the fetched provider TINs.
#     4. Save the query results to SharePoint.

# Logging:
#     - The script uses the logging module to log important events, including successful operations and errors.
#     - Logs are configured to include timestamps, log levels, and messages.

from PROVIDER.functions_provider_utils import load_config, fetch_data_from_sharepoint, fetch_data_from_SQL, save_to_sharepoint, logger
import datetime


def export_report():
    try:
        # Load Config from the provided path
        config_path = 'config_provider_module.ini'
        config = load_config(config_path)

        # Retrieve database and SharePoint Config settings
        db_config = config['DatabaseConfig']
        sharepoint_config = config['SharePointConfig']

        # Fetch data from SharePoint
        data_df, ctx = fetch_data_from_sharepoint(
            site_url=sharepoint_config['ProviderAnalyticsFileExchange'],
            file_url=sharepoint_config['oh_tins_path'],
            username=sharepoint_config['username'],
            password=sharepoint_config['password'],
            file_format='csv'
        )

        # Extract provider TINs from the fetched data and format them for the SQL query
        provider_tins = data_df['PROVIDERTIN'].tolist()
        provider_tins_str = ', '.join([f"'{tin}'" for tin in provider_tins])

        # Construct the SQL query using the provider TINs
        query = f"""
        SELECT * 
        FROM 
            [].[].[]
        WHERE 
            PROVIDERTIN IN ({provider_tins_str})
        """

        # Execute the SQL query and fetch the data
        df = fetch_data_from_SQL(db_config, query)

        # Check if the query returned any data
        if not df.empty:
            # Generate a filename with the current date
            today = datetime.date.today().strftime('%Y%m%d')
            file_name = f'{today}_OH_tins_pir.xlsx'
            
            # Save the data to SharePoint
            save_to_sharepoint(ctx, df, sharepoint_config['oh_report_path'], file_name)
            logger.info("Data successfully exported and saved to SharePoint.")
        else:
            # Log if no data was found
            logger.info("No data found for the provider TINs.")

        # Log the provider TINs and script completion
        logger.info("Script execution complete.")

    except Exception as e:
        # Log any errors that occur during execution
        logger.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    export_report()
