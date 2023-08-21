from anchor import *
import pandas as pd
import snowflake.connector


import re
import json
import pandas as pd
import snowflake.connector

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user='anandbc2',
    password='Anand@3159',
    account='tg93126.europe-west4.gcp'
)

db = 'DEV_DG_DATABASE'
schema = 'DEV_DG_SCHEMA'
warehouse = 'DEV_DG_WAREHOUSE'
role = 'ACCOUNTADMIN'

cur = conn.cursor()
set_context(cur, db, schema, warehouse, role)

# Example JSON data

query = (f"select table_name, column_name, alternates from classification_results_main where alternates != '[]'")
print(query)
cur.execute(query)
data = []
for i in cur.fetchall():
    data.append(i)

# Define column names for the DataFrame
columns = ['table_name', 'column_name', 'json_data']

# Create a Pandas DataFrame
df = pd.DataFrame(data, columns=columns)

# Flatten the JSON data and convert it into separate records
flattened_records = []
for index, row in df.iterrows():
    table_name = row['table_name']
    column_name = row['column_name']
    json_data = row['json_data']

    # Remove newline characters from alternates using regex
    alternates_cleaned = re.sub(r'\n', '', json_data)

    # Parse the cleaned alternates as JSON
    try:
        parsed_json = json.loads(alternates_cleaned)
        for record in parsed_json:
            new_record = {
                'table_name': table_name,
                'column_name': column_name,
                **record  # Include key-value pairs from the parsed JSON
            }
            flattened_records.append(new_record)
    except json.JSONDecodeError:
        print(f"Error decoding JSON: {alternates_cleaned}")

# Create a new DataFrame from the flattened records
flattened_df = pd.DataFrame(flattened_records)

print(flattened_df)



