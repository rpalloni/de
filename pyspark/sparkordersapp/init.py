import os
import requests
import psycopg2
from utils import db

base_local_path = 'data'
base_remote_path = 'https://raw.githubusercontent.com/rpalloni/dataset/master/de'
manifest_url = f'{base_remote_path}/manifest'
data_urls = requests.get(manifest_url, allow_redirects=True).content.decode('utf-8')
print(data_urls)

for url in data_urls.splitlines():
    content = requests.get(url, allow_redirects=True).content.decode('utf-8')
    local_path = url.replace(base_remote_path, base_local_path)
    print(local_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'w+') as f:
        f.write(content)

with psycopg2.connect(
        host=db.DB_HOST,
        user=db.DB_USER,
        password=db.DB_PWD,
        dbname=db.DB_NAME
        ) as connection:
    with open('data/init.sql') as f:
        sql = f.read()
    with connection.cursor() as cursor:
        cursor.execute(sql)
