import requests
import pandas as pd
import json
import time
import psycopg2

from psycopg2 import sql
from datetime import datetime, timedelta
from db_connection import create_connection
import os


soda_cloud_url = os.getenv("SODA_CLOUD_URL") # Use cloud.soda.io for EU region
soda_apikey = os.getenv("SODA_CLOUD_API_KEY")
soda_apikey_secret = os.getenv("SODA_CLOUD_API_SECRET")

response = requests.get(
    f'{soda_cloud_url}/api/v1/checks?size=100',
    auth=(soda_apikey, soda_apikey_secret)
)
checks_table = 'narjes_certif_soda_checks'
print(f"response: {response}")
if response.status_code == 200:
    message = f"Checks have been written to the {checks_table} table in PostgreSQL."
    checks_pages = response.json().get('totalPages')
    i = 0
    checks = []
    
    while i < checks_pages:
        dq_checks = requests.get(
            soda_cloud_url + '/api/v1/checks?size=100&page=' + str(i), 
            auth=(soda_apikey , soda_apikey_secret))

        if dq_checks.status_code == 200:
            print("Fetching all checks on page " + str(i))
            check_list = dq_checks.json().get("content")
            checks.extend(check_list)
            i += 1
        elif dq_checks.status_code == 429:
            print("API Rate Limit reached when fetching checks on page: " + str(i) + ". Pausing for 30 seconds.")
            time.sleep(30)
            # Retry fetching the same page
        else:
            print("Error fetching checks on page " + str(i) + ". Status code:", dq_checks.status_code)
    
    df_checks = pd.DataFrame(checks)

    # Convert complex data types to JSON strings
    for column in df_checks.columns:
        if df_checks[column].dtype == 'object':
            df_checks[column] = df_checks[column].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Create PostgreSQL connection using psycopg2
    connection = create_connection()
    
    if connection is not None:
        cursor = connection.cursor()

        # Prepare the SQL query for inserting or updating data
        columns = df_checks.columns.tolist()
        quoted_columns = [f'"{col}"' for col in columns]
        columns_str = ', '.join(quoted_columns)
        values_placeholder = ', '.join(['%s'] * len(columns))

        # Define the conflict action: if the id exists, update the columns
        conflict_action = sql.SQL("ON CONFLICT (id) DO UPDATE SET {}").format(
            sql.SQL(', ').join(
                [sql.SQL(f'"{col}" = EXCLUDED."{col}"') for col in columns]
            )
        )

        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) {}").format(
            sql.Identifier(checks_table),
            sql.SQL(columns_str),
            sql.SQL(values_placeholder),
            conflict_action
        )

        try:
            # Insert data row by row into PostgreSQL
            for row in df_checks.itertuples(index=False, name=None):
                if "Schema Check" in row or "anomaly score for row_count < default" in row: 
                    continue
                cursor.execute(insert_query, row)
            
            connection.commit()  # Commit the transaction
            print(f"Data has been written to the {checks_table} table in PostgreSQL.")

        except Exception as e:
            print(f"Error inserting/updating data into PostgreSQL: {e}")
            message = f"Error inserting/updating data into PostgreSQL: {e}"
            connection.rollback()

        finally:
            cursor.close()
            connection.close()

    else:
        print("Failed to create a connection to PostgreSQL.")
        message = f"Failed to create a connection to PostgreSQL."

    print(message)

else:
    print(f"Error fetching initial checks. Status code: {response.status_code}")
