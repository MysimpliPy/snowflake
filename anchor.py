from login_logout_context import *



def setup() :
    try :
        print('calling setup ....')
        db        = 'DEV_DG_DATABASE'
        schema    = 'DEV_DG_SCHEMA'
        warehouse = 'DEV_DG_WAREHOUSE'
        role      = 'ACCOUNTADMIN'
        try:
            conn = connect_to_snowflake()
            if conn:
                cur = conn.cursor()
                set_context(cur, db, schema, warehouse, role )
            ##  disconnect_from_snowflake(conn)
        except Exception as e:
            print('ERROR while connecting to snowflaken ->', e)
    except :
         print('error.....')
    return conn, db, schema, warehouse, role

    