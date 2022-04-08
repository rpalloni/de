from dags.config import db

conn = db.get_connection()

create_os_table_query = '''
CREATE TABLE IF NOT EXISTS osdata(
    id serial PRIMARY KEY,
    name VARCHAR(100),
    version VARCHAR(100),
    install VARCHAR(100),
    owner VARCHAR(100),
    kernel NUMERIC(11,2)
);
'''

with conn.cursor() as cursor:
    cursor.execute(create_os_table_query)
    conn.commit()


insert_os_query = '''
INSERT INTO osdata (name, version, install, owner, kernel)
VALUES
    ('Debian', '9', 'apt', 'SPI', 4.9),
    ('Ubuntu', '17.10', 'apt', 'Canonical', 17.4)
'''

with conn.cursor() as cursor:
    cursor.execute(insert_os_query)
    conn.commit()


conn.close()