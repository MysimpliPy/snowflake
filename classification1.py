from anchor import *
from decimal import Decimal

def extract(cur, db, schema) :
    table_name = 'EMPLOYEE_MASTER1'

    column_name = 'SSN'
    print(  db )
    print(schema)
    cur.execute(f'use database {db}')
    cur.execute(f'use schema {schema}')
    query = f'''SELECT
    f.key::varchar as column_name,
    f.value:"privacy_category"::varchar as privacy_category,
    f.value:"semantic_category"::varchar as semantic_category,
    f.value:"extra_info"::variant:"probability"::number(10,2) as probability,
    f.value:"extra_info"::variant:"alternates" as alternates
    FROM TABLE(FLATTEN(INPUT => EXTRACT_SEMANTIC_CATEGORIES('{db}.{schema}.{table_name}')::VARIANT)) AS f'''

    cur.execute(query)

    for fields in cur.fetchall() :
        x = [table_name] + list(fields)
        for i in range(len(x)) :
            if x[i] == None :
                x[i] = 'null'
        if x[4] == 'null' :
            print(x[4]) 
            x[4] = 0
        if isinstance(x[4], Decimal):
            x[4] = float(x[4])

        print(x)
        cur.execute(f'insert into classification_results_main values {tuple(x)}')   
        #print(f'insert into classification_results_main values {tuple(x)}')   
        #print('hello done!')

if  __name__ == '__main__' :    

    try :
        conn, db, schema, warehouse, role = setup()
        cur = conn.cursor()
        print('calling extract ....')
        extract(cur, db, schema)
        cur.close()
        disconnect_from_snowflake(conn)
    except Exception as e :
        print('Error in main function -> ', e)   