import pandas as pd
import configparser
import snowflake.connector

config = configparser.ConfigParser()
config.read(r"settings_75249.ini") 

try:
    conn = snowflake.connector.connect(
        account=config["snowflake"]["account"],
        user=config["snowflake"]["user"],
        password=config["snowflake"]["password"],
        database=config["snowflake"]["database"],
        warehouse=config["snowflake"]["warehouse"],
        schema=config["snowflake"]["schema"]
    )
    print('Connected to SF')
except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")

# Extract data from source in SF
cursor = conn.cursor()
cursor.execute('select top 1* from analytics.public.incident_tracker')

try:
# Get data
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description] 

    # Create df
    df = pd.DataFrame(data, columns=column_names)

    print('df extracted from SF')
    print(df)
except Exception as e:
        print(f"Error: {str(e)}")

# Close connection with SF

#df.to_csv('columnas_incident_tracker.csv',sep=',', index=False, encoding='utf-8')

conn.close()



''' dunción para subir a snf
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
        remove_files(folder_path_new)
    except Exception as e:
            print(f"Error uploading data: {str(e)}")
'''