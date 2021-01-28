#!/usr/bin/python
import pymysql
import logging
from datetime import datetime

from pathlib import Path
import sys
current_path=Path().resolve()
sys.path.append(str(current_path))

from config.Config import db_config


def connect_dbSever(db_config):
    try:
        connect = pymysql.connect(user=db_config.user,  password=db_config.password, host=db_config.host,port=int(db_config.port),
                                    ssl={'ca': db_config.pem_file_path}, ssl_disabled=False)
        cursor = connect.cursor()
        return connect,cursor
    except Exception as e:
        logging.error('Fail to connection mysql {}'.format(str(e)))
    return None
#connect,cursor=connect_dbSever(db_config)


def create_schema(db_config,schemaName):
    connect, cursor = connect_dbSever(db_config)
    cursor.execute(f"CREATE DATABASE {schemaName}")
    cursor.execute("SHOW DATABASES")

    for x in cursor:
      print(x)
    connect.close()

schemaName='usstock'
create_schema(db_config,schemaName)

#
# def show_db_table(dbName):
#     connect, cursor = connectDb(dbName)
#     if cursor is None:
#         print('cursor object is None, please check connection')
#         connect.close()
#     else:
#         cursor.execute("show tables")
#         table_list = [tuple[0] for tuple in cursor.fetchall()]
#         connect.close()
#         print(table_list)
#         return table_list
#
#
# #show_db_table(dbName)
#
# def create_table(tableName):
#     connect, cursor = connectDb(dbName)
#     if cursor is None:
#         print('cursor object is None, please check connection')
#     else:
#         cursor.execute("show tables")
#         table_list_before = [tuple[0] for tuple in cursor.fetchall()]
#
#         sql = f'''CREATE TABLE IF NOT EXISTS `{tableName}`( \
#         `timestamp` DATETIME PRIMARY KEY, \
#         `open` FLOAT NOT NULL, \
#         `high` FLOAT NOT NULL, \
#         `low` FLOAT NOT NULL ,\
#         `close` FLOAT NOT NULL, \
#         `volume` FLOAT NOT NULL, \
#         `num_trade` INT NOT NULL )ENGINE=InnoDB'''
#
#         cursor.execute(sql)
#
#         cursor.execute("show tables")
#         table_list_after = [tuple[0] for tuple in cursor.fetchall()]
#         table_added= list(set(table_list_after) - set(table_list_before))
#
#         if table_added:
#             print(f'add new table : {table_added}')
#         else:
#             print('No table is added')
#         connect.close()


# tableName='Binance_BTCUSDT_daily'
# create_table(tableName)


def insert_records(records,tableName):
    connect, cursor = connectDb(dbName)
    if cursor is None:
        print('cursor object is None, please check connection')
    else:
        try:
            sql = f'''insert ignore into {tableName}(timestamp,open, high, low, close, volume,  num_trade) values (%s, %s, %s, %s, %s, %s, %s)'''
            cursor.executemany(sql, records)
            connect.commit()
            print(f'insert {len(records)} into {tableName} at {datetime.now()}')

        except pymysql.Error as e:
            connect.rollback()
            print("insert error pymysql %d: %s" % (e.args[0], e.args[1]))

        connect.close()
