from anchor import *

#################>> 

# Brief on usage of this function :
# prerequisite :  database tags should be in place, hence run tag_creation.py before this
# Make sure to provide the appropriate values for 'cur', 'db', and 'schema'
# assign_tag(cur, 'db_name', 'schema_name')

def assign_tag(cur, db, schema) :

    try :
        cur.execute(f"SELECT * FROM {db}.{schema}.TAG_ASSIGNMENT_MASTER")
        
        print(f'assignment in progress : ')
        for vars in  cur.fetchall() :
            #print(f'assignment in progress :  {vars}')
            query       = f"ALTER TABLE {db}.{schema}.{vars[0]} MODIFY COLUMN {vars[1]} SET TAG {vars[2]} = '{vars[3]}'"
            #cur.execute(query)
        print('assignment successful')
  
    except Exception as e:
        print('Exception in assign_tag function:', e)

#################<<
    
if __name__ == "__main__":
    try :
        conn, db, schema, warehouse, role = setup()
        cur = conn.cursor()
        assign_tag(cur, db, schema)
        cur.close()
        disconnect_from_snowflake(conn)
    except Exception as e :
        print('Error in main function -> ', e)    