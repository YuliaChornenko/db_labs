import time

import psycopg2

from db.create import create
from db.insert import insert
from db.select import select
from db.db_config import db_config

print("Чорненко Юлії, КМ-83, Варіант 8")

# кількість повторних спроб
attempt = 5

while attempt:

    try:

        connection = psycopg2.connect(**db_config)
        break

    except Exception as err:

        print(err)
        attempt -= 1
        print(f'Повторна спроба №{attempt}')
        time.sleep(1)

try:

    create(connection)
    insert(connection)
    select(connection)

except Exception as err:
    print(err)

if connection:
    connection.close()