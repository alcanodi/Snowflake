# Snowflake connection
import configparser
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

config = configparser.ConfigParser()
config.read(r"/settings.ini") # Update '' with the settings file path
snowflake_table = 'Table name in SF to update' # Place the table name in SF to be updated

def upload_to_SF(config, df, snowflake_table):
    try:
        conn = snowflake.connector.connect(
            account=config["snowflake"]["account"],
            user=config["snowflake"]["user"],
            password=config["snowflake"]["password"],
            database=config["snowflake"]["database"],
            warehouse=config["snowflake"]["warehouse"],
            schema=config["snowflake"]["schema"]
        )
        print('Connected to Snowflake')
    except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")

    try:
        snowflake.connector.pandas_tools.write_pandas(conn, df, snowflake_table)
        print('Data uploaded sucessfully')
    except Exception as e:
            print(f"Error uploading data: {str(e)}")