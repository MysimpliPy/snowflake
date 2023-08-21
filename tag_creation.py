from anchor import *


'''
A brief on usage of this function :
> Make sure to provide the appropriate values for 'cur', 'db', and 'schema'
> create_tags_and_allowed_values(cur, 'db_name', 'schema_name')
'''

def create_tags_and_allowed_values(cur, db, schema):

    try:
        # Fetch tag variables from the table
        cur.execute(f'SELECT * FROM {db}.{schema}.TAG_MASTER')
        
        # Iterating through each tag variable
        for vars in cur.fetchall() :
            query_create_tag = f'CREATE TAG IF NOT EXISTS {db}.{schema}.{vars[0]}'
            #print(query_create_tag)
            #cur.execute(query_create_tag)

            # Processing allowed_values
            for allowed_value in vars[1].split(',') :
                query_add_allowed_value = f'ALTER TAG IF EXISTS {db}.{schema}.{vars[0]} ADD ALLOWED_VALUES \'{allowed_value.strip()}\''
                cur.execute(query_add_allowed_value)

        print(f"'tags and allowed_values' created successfully.")

    except Exception as e:
        print("ERROR in create_tags_and_allowed_values function -> ", str(e))
    
    

    except Exception as e:
        print('Exception:', e)


if  __name__ == '__main__' :

    try :
        
        conn, db, schema, warehouse, role = setup()
        cur = conn.cursor()
        create_tags_and_allowed_values(cur, db, schema)
        cur.close()
        disconnect_from_snowflake(conn)
    except Exception as e :
        print('Error in main function -> ', e)    