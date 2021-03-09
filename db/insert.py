import os
import re
import csv
import time

import psycopg2
import psycopg2.extras

from db.db_config import db_config


BATCH_SIZE = 50

# запит для заповнення таблиці
QUERY2 = (
    """
        INSERT INTO zno_analytics
        ( year, OUTID, Birth, SEXTYPENAME, REGNAME,
          AREANAME, TERNAME, REGTYPENAME, TerTypeName, ClassProfileNAME, 
          ClassLangName, EONAME, EOTYPENAME, EORegName, EOAreaName,
          EOTerName, EOParent, UkrTest, UkrTestStatus, UkrBall100,
          UkrBall12, UkrBall, UkrAdaptScale, UkrPTName, UkrPTRegName,
          UkrPTAreaName, UkrPTTerName, histTest, HistLang, histTestStatus, 
          histBall100, histBall12, histBall, histPTName, histPTRegName,
          histPTAreaName, histPTTerName, mathTest, mathLang, mathTestStatus,
          mathBall100, mathBall12, mathBall, mathPTName, mathPTRegName,
          mathPTAreaName, mathPTTerName, physTest, physLang, physTestStatus,
          physBall100, physBall12, physBall, physPTName, physPTRegName,
          physPTAreaName, physPTTerName, chemTest, chemLang, chemTestStatus,
          chemBall100, chemBall12, chemBall, chemPTName, chemPTRegName,
          chemPTAreaName, chemPTTerName, bioTest, bioLang, bioTestStatus,
          bioBall100, bioBall12, bioBall, bioPTName, bioPTRegName,
          bioPTAreaName, bioPTTerName, geoTest, geoLang, geoTestStatus,
          geoBall100, geoBall12, geoBall, geoPTName, geoPTRegName,
          geoPTAreaName, geoPTTerName, engTest, engTestStatus, engBall100,
          engBall12, engDPALevel, engBall, engPTName, engPTRegName,
          engPTAreaName, engPTTerName, fraTest, fraTestStatus, fraBall100,
          fraBall12, fraDPALevel, fraBall, fraPTName, fraPTRegName,
          fraPTAreaName, fraPTTerName, deuTest, deuTestStatus, deuBall100,
          deuBall12, deuDPALevel, deuBall, deuPTName, deuPTRegName,
          deuPTAreaName, deuPTTerName, spaTest, spaTestStatus, spaBall100,
          spaBall12, spaDPALevel, spaBall, spaPTName, spaPTRegName,
          spaPTAreaName, spaPTTerName
        ) VALUES %s
    """,
    """
        UPDATE help_table SET id = %s, done = %s WHERE year = %s
    """
)


# функція для зчитування данних з файлів та заповнення таблиці
def insert(conn):

    cursor = conn.cursor()

    # словник для запису часу виконання запитів запису
    executions_time = dict()

    # почергово зчитуємо дані з файлів
    for file in os.listdir('../data'):

        y = re.findall(r'Odata(\d{4})File.csv', file)

        if y:
            with open(os.path.join('../data', file), encoding='cp1251') as csvfile:
                # пропускаємо заголовок
                reader = csv.reader(csvfile, delimiter=';')
                next(reader)

                # лічильник часу виконання
                start = time.time()
                idx = 0
                batch = list()

                cursor.execute('SELECT id, done FROM help_table WHERE year = %s', y)
                res = cursor.fetchone()

                if res is None:
                    cursor.execute('INSERT INTO help_table (year, id, done) VALUES (%s, %s, %s)', y + [idx, False])
                else:
                    if res[-1]:
                        print(f'{file}...')
                        continue
                    for r in reader:
                        idx += 1
                        if idx >= res[0]:
                            break

                # підготовка данних
                print(f'{file}...')
                for r in reader:

                    for i in range(len(r)):
                        
                        if r[i] == 'null':
                            
                            r[i] = None
                            
                        else:
                            
                            if i in [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 
                                     70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]:
                                
                                r[i] = r[i].replace(',', '.')
                                r[i] = float(r[i])

                    idx += 1
                    batch.append(y + r)
                    
                    if not idx % BATCH_SIZE:
                        psycopg2.extras.execute_values(cursor, QUERY2[0], batch)
                        cursor.execute(QUERY2[1], [idx, False] + y)
                        batch = list()
                        conn.commit()
                        
                if batch:
                    
                    psycopg2.extras.execute_values(cursor, QUERY2[0], batch)
                    batch = list()

                cursor.execute(QUERY2[1], [idx, True] + y)
                conn.commit()
                exec_time = time.time() - start
                print(f'{file} оброблено')
                executions_time[file] = exec_time

    print('Таблиці заповнено')
    cursor.close()

    if executions_time:
        with open(os.path.join('../data', 'time.txt'), 'w') as file:
            file.write(str(executions_time))


if __name__ == '__main__':
    
    connection = psycopg2.connect(**db_config)
    insert(connection)
    connection.close()
