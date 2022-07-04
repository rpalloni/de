import snowflake.connector as snow

conn = snow.connect(
    user='<user>',
    password='<pwd>',
    account='<account>' # xyz.us-east-1
)

conn.cursor().execute("USE DATABASE cookbook")

# add data from local file
conn.cursor().execute("PUT file:///tmp/snowflaketmp/usersdata.csv @%users")
conn.cursor().execute("COPY INTO users")
