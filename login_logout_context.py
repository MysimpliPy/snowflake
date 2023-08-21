
import snowflake.connector as sc

def connect_to_snowflake():
    try:

        conn      = sc.connect(user      = 'anandbc2',
                               password  = 'Anand@3159',
                               account   = 'tg93126.europe-west4.gcp')
        print('Connected to Snowflake.')
        return conn
    except Exception as e:
        print('ERROR while connecting to Snowflake:', e)
        return None

def disconnect_from_snowflake(conn):
    try:
        if conn:
            conn.close()
            print('Disconnected from Snowflake.')
        else:
            print('No active connection to disconnect.')
    except Exception as e:
        print('ERROR while disconnecting from Snowflake ->', e)


def set_context(cur, db, schema, warehouse, role) :
    try:
        cur.execute(f'USE DATABASE {db}')
        cur.execute(f'USE SCHEMA {db}.{schema}')
        cur.execute(f'USE WAREHOUSE {warehouse}')
        cur.execute(f'USE ROLE {role}')
        print('context has been set.')
    except Exception as e:
        print('ERROR in set_context function ->', e)
