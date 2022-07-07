import snowflake.connector as snow

conn = snow.connect(
    user='<user>',
    password='<pwd>',
    account='<account>' # xyz.us-east-1
)

conn.cursor().execute('USE DATABASE cookbook')

# add data from local file
conn.cursor().execute('PUT file:///tmp/data/usersdata.csv @%users')
conn.cursor().execute('COPY INTO users')

result = conn.cursor().execute('SELECT * FROM CARDSDATA').fetchone()


results = conn.cursor().execute('SELECT * FROM CARDSDATA').fetchall()
[r for r in results] # list of records
[r[1] for r in results] # list of cc numbers


from snowflake.connector import DictCursor
dictcur = conn.cursor(DictCursor)

res = dictcur.execute('SELECT * FROM CARDSDATA').fetchall()
[r['CUSTOMER_NAME'] for r in res] # use column name

dictcur.sfqid # get snowflake query id

dictcur.get_results_from_sfqid(dictcur.sfqid)
dictcur.fetchall()


# binding parameter to variable for batch insert (multiple rows in a single insert)
rows_to_insert = [
    ('Brian Wallace', '9323391001765046', 'Discover', 685, '02/27'),
    ('Donald Kerry', '4610093233965017', 'Mastercard', 322, '05/25'),
    ('Brian Terry', '3391650469320017', 'American Express', 526, '03/17'),
    ('Clinton Mark', '4696503230173910', 'VISA 19 digit', 785, '09/25'),
    ('Luke Perry', '1001765046932339', 'JCB 15 digit', 124, '02/23')
]

conn.cursor().executemany('insert into CARDSDATA (customer_name, credit_card, type, ccv, exp_date) values (%s, %s, %s, %s, %s)', rows_to_insert)
conn.cursor().execute('SELECT count(*) FROM CARDSDATA').fetchone()


conn.close()
