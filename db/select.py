import os
import csv

import psycopg2

from db.db_config import db_config


# запит відповідно до варіанту
QUERY3 = """
    SELECT 
        year, 
        Regname,
        avg(mathBall)
    FROM zno_analytics 
    WHERE physTestStatus = 'Зараховано' 
    GROUP BY year, Regname
"""

# задаємо назви колонок
column_names = ['Year', 'RegName', 'Ball']
result = 'result.csv'


# функція для виконання запиту
def select(conn):
    cursor = conn.cursor()

    cursor.execute(QUERY3)
    r = cursor.fetchall()

    # створюємо файл для запису результату
    with open(os.path.join('../data', result), 'w', newline='') as file:
        w = csv.writer(file, dialect='excel')
        w.writerow(column_names)
        w.writerows(r)

    print(f'Результат запиту записано до файлу')

    cursor.close()


if __name__ == '__main__':
    connection = psycopg2.connect(**db_config)
    select(connection)
    connection.close()
