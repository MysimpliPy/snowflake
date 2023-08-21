from anchor import *

#################>> 

# Brief on usage of this function :
# prerequisite :  database tags should be in place, hence run tag_creation.py before this
# Make sure to provide the appropriate values for 'cur', 'db', 'tag_schema', 'mask_schema', mask_name 
# assign_tag(cur, 'db_name', 'schema_name')


def create_masking_policy_assignment(cur, db, schema) :

    try :
        cur.execute(f"SELECT * FROM  {db}.{schema}.MASKING_POLICY_ASSIGNMENT_MASTER")
        for vars in  cur.fetchall() :
            #print(vars)
            print(f'assignment in progress :  {vars}')
            query = f"alter tag {db}.{schema}.{vars[0]}  set masking policy {db}.{schema}.{vars[1]}"
           #print(query)
            cur.execute(query)
            print('assignment successful')  

    except Exception as e:
        print('ERROR in create_masking_policy_assignment function ->', e)

  
#################<<
    
if __name__ == "__main__":
    try :
        conn, db, schema, warehouse, role = setup()
        cur = conn.cursor()
        create_masking_policy_assignment(cur, db, schema)
        cur.close()
        disconnect_from_snowflake(conn)
    except Exception as e:
        print('Exception:', e)
