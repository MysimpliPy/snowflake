from anchor import *
import hashlib as hash

#################>> 

# Brief on usage of this function :
# prerequisite :  database tags should be in place, hence run tag_creation.py before this
# Make sure to provide the appropriate values for 'cur', 'db', 'tag_schema', 'mask_schema', mask_name 
# assign_tag(cur, 'db_name', 'schema_name')


# masking_policies.py
def create_masking_policy(cur, db, schema):
    
    try:
      # Fetch mask variables from the table
        cur.execute(f'SELECT * FROM {db}.{schema}.MASKING_POLICY_MASTER')
        # Iterating through each tag variable
        print()
        for fields in cur.fetchall() :

            mask_value = f'\'{fields[2]}\'' if fields[1] == 'STRING' else int(fields[2]) 
           # mask_value = hash.sha256(mask_value)
            print(f'defining masking policy {fields[0]} ...')
            create_query = f'''CREATE MASKING POLICY IF NOT EXISTS {db}.{schema}.{fields[0]} AS (value {fields[1]}) RETURNS {fields[1]} ->
                           CASE
                               WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN value
                           ELSE {mask_value}
                           END;'''
            cur.execute(create_query)
            print(f'defined!')
            print()    
    except Exception as e:          
            print('ERROR while creating masking policy:', e)

if __name__ == "__main__":
    try :
        conn, db, schema, warehouse, role = setup()
        cur = conn.cursor()
        create_masking_policy(cur, db, schema)
        cur.close()
        disconnect_from_snowflake(conn)   

    except Exception as e:
        print('ERROR while calling main function ->', e)