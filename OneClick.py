from anchor import *

def create_tags_and_allowed_values(cur, db, schema):

    try:
        # Fetch tag variables from the table
        cur.execute(f'SELECT * FROM {db}.{schema}.TAG_MASTER')
        
        # Iterating through each tag variable
        for vars in cur.fetchall() :
            query_create_tag = f'CREATE TAG IF NOT EXISTS {db}.{schema}.{vars[0]}'
            print(query_create_tag)
            cur.execute(query_create_tag)

            # Processing allowed_values
            for allowed_value in vars[1].split(',') :
                query_add_allowed_value = f'ALTER TAG IF EXISTS {db}.{schema}.{vars[0]} ADD ALLOWED_VALUES \'{allowed_value.strip()}\''
                cur.execute(query_add_allowed_value)

        print(f"'tags and allowed_values' created successfully.")

    except Exception as e:
        print("ERROR in create_tags_and_allowed_values function -> ", str(e))
    

def assign_tag(cur, db, schema) :

    try :
        cur.execute(f"SELECT * FROM {db}.{schema}.TAG_ASSIGNMENT_MASTER")
        
        for vars in  cur.fetchall() :
        
            print(f'tag assignment in progress :  {vars}')
            query       = f"ALTER TABLE {db}.{schema}.{vars[0]} MODIFY COLUMN {vars[1]} SET TAG {vars[2]} = '{vars[3]}'"
            cur.execute(query)
            print('tag assignment successful')
  
    except Exception as e:
        print('Exception in assign_tag function:', e)

    
def create_masking_policy(cur, db, schema):
    
    try:
      # Fetch mask variables from the table
        cur.execute(f'SELECT * FROM {db}.{schema}.MASKING_POLICY_MASTER')
        # Iterating through each tag variable
        print()
        for fields in cur.fetchall() :
            mask_value = f'\'{fields[2]}\'' if fields[1] == 'STRING' else int(fields[2]) 
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


def create_masking_policy_assignment(cur, db, schema) :

    try :
        cur.execute(f"SELECT * FROM  {db}.{schema}.MASKING_POLICY_ASSIGNMENT_MASTER")
        for vars in  cur.fetchall() :
            #print(vars)
            print(f'masking policy assignment in progress :  {vars}')
            query = f"alter tag {db}.{schema}.{vars[0]}  set masking policy {db}.{schema}.{vars[1]}"
           #print(query)
            cur.execute(query)
            print('masking policy assignment successful')  

    except Exception as e:
        print('ERROR in create_masking_policy_assignment function ->', e)

   
if __name__ == "__main__":
    try :
        conn, db, schema, warehouse, role = setup()
        cur = conn.cursor()
        create_tags_and_allowed_values(cur, db, schema)
        assign_tag(cur, db, schema)
        create_masking_policy(cur, db, schema)
        create_masking_policy_assignment(cur, db, schema)
        cur.close()
        disconnect_from_snowflake(conn)
    except Exception as e:
        print('Exception:', e)
