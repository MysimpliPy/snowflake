from anchor import *
from decimal import Decimal


def extract(cur, db, schema) :
    
 
    cur.execute(f'use database {db}')
    cur.execute(f'use schema {schema}')


    cur.execute('delete from classification_results')
    cur.execute(f"select table_name from information_schema.tables where table_type = 'BASE TABLE'")
    #cur.execute(f"select table_name from information_schema.tables where table_name = 'MYTAB' AND  table_type = 'BASE TABLE'")
    
    tab = []
    for i in cur.fetchall() :
        print(i[0])
        tab.append(i[0])

    for i in tab :
        query = f'''
with temp
as
(
SELECT
    '{i}' table_name,
    f.key::varchar as column_name,
    f.value:"privacy_category"::varchar as privacy_category,
    f.value:"semantic_category"::varchar as semantic_category,
    f.value:"extra_info"::variant:"probability" as probability,
    f.value:"extra_info"::variant:"alternates"[0]:"privacy_category"::varchar as a_privacy_category,
    f.value:"extra_info"::variant:"alternates"[0]:"semantic_category"::varchar as a_semantic_category,
    f.value:"extra_info"::variant:"alternates"[0]:"probability" as a_probability
FROM TABLE(FLATTEN(INPUT => EXTRACT_SEMANTIC_CATEGORIES('{i}')::VARIANT)) AS f
)
SELECT
    table_name,
    column_name,
    privacy_category,
    semantic_category,
    cast(probability as number(5,2)) probability
    from temp
    where a_privacy_category is null
    union all
SELECT
    table_name,
    column_name,
    a_privacy_category,
    a_semantic_category,
    cast(a_probability as number(5,2)) probability 
from temp 
where a_privacy_category is not null'''
        #print(query)
        cur.execute(query)  
        for i in cur.fetchall() : 
            
            if i[4] == None :
                x = 0
            else :
                x = i[4]
    
            #query = f"insert into classification_results values('{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', {x})"
            query = f"insert into classification_results values('{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', {0 if i[4] == None else i[4]})"
            print(query)



            try :
                cur.execute(query)  
                pass
            except :
                print('ERROR..... while inserting', query)
            print(query)    

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
    print(Decimal('9.0'))