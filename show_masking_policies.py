import snowflake.connector as sc

conn = sc.connect(
    user      =   'anandbc2',
    password  =   'Anand@3159',
    account   =   'tg93126.europe-west4.gcp'
)

cur = conn.cursor()

query = "SHOW MASKING POLICIES LIKE '%MASK%'"
cur.execute(query)

for row in cur.fetchall():
    print(row[1])   

cur.close()
conn.close()