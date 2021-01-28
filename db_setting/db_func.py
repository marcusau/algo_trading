#!/usr/bin/python
import pymysql
import logging
from datetime import datetime

from pathlib import Path
import sys
current_path=Path().resolve()
sys.path.append(str(current_path))

from config.Config import db_config,data_source_config



##############  Connect to Azure SQL server ###################################################################################

def connect_dbSever():
    try:
        connect = pymysql.connect(user=db_config.user,  password=db_config.password, host=db_config.host,port=int(db_config.port),    ssl={'ca': db_config.pem_file_path}, ssl_disabled=False)
        cursor = connect.cursor()
        return connect,cursor
    except Exception as e:
        logging.error('Fail to connection mysql {}'.format(str(e)))
        print('Fail to connection mysql {}'.format(str(e)))
    return None,None
# print('Connecting to Azure DB ')
# connect,cursor=connect_dbSever(db_config)

##############  Connect to Azure SQL schama ###################################################################################

def connect_db( schema:str):
    assert schema in ['usstock', 'hkstock', 'binance'] , f" {schema} are not within existing db, please create one "
    try:
        connect = pymysql.connect(user=db_config.user, password=db_config.password, host=db_config.host, port=int(db_config.port), ssl={'ca': db_config.pem_file_path}, ssl_disabled=False, db=db_config.schema[schema])
        cursor = connect.cursor()
        return connect,cursor
    except Exception as e:
        logging.error('Fail to connection mysql {}'.format(str(e)))
    return None
# print(f'Connecting to Azure DB  SchemaName: {db_config.schema.usstock}')
# connect,cursor=connect_db(db_config=db_config)
# if connect is not None and cursor  is not None:
#     print('success ')

##############  Create schema  ###################################################################################
def create_schema(schemaName):
    connect, cursor = connect_dbSever()
    try:
        cursor.execute(f"CREATE DATABASE {schemaName} ")
        cursor.execute("SHOW DATABASES")
        print(f'Schema : {schemaName} is created ')
    except Exception as e:
        print(f' cannot create DATABASE {schemaName} because of {e}')
    connect.close()

#schemaName='usstock'
#create_schema(db_config,schemaName)

######### show table in db ###################################################################################

def show_db_table(schema:str):
    connect, cursor = connect_db( schema=schema)
    if cursor is None:
        print('cursor object is None, please check connection')
        connect.close()
    else:
        cursor.execute("show tables")
        table_list = [tuple[0] for tuple in cursor.fetchall()]
        connect.close()
        if len(table_list)==0:
            print(f'there is no table in Schema : {schema}')
            logging.info(f'there is no table  Schema :  {schema}')
            return []
        else:
            print(f'tablenames: {table_list}  Schema :  {schema}')
            logging.info(f'tablenames: {table_list}  Schema :  {schema}')
            return table_list

#show_db_table(db_config=db_config,market='usstock')

######### Create New Table and Add Index ################################################

######### Create New Table  ################################################

def create_table(tablename:str,schema:str ):
    connect, cursor = connect_db(schema=schema)
    if cursor is None:
        print('cursor object is None, please check connection')
    else:
        try:

            sql = f'''CREATE TABLE IF NOT EXISTS `{tablename}`( \
                  `timestamp` DATETIME, \
                  `ticker` CHAR(30) NOT NULL , \
                  `open` FLOAT NOT NULL, \
                  `high` FLOAT NOT NULL, \
                  `low` FLOAT NOT NULL ,\
                  `close` FLOAT NOT NULL, \
                  `volume` FLOAT NOT NULL,\
                 PRIMARY KEY (timestamp, ticker))ENGINE=InnoDB'''

            cursor.execute(sql)

            print(f'add new table : {tablename} in Schema : {schema}')
        except Exception as e:
            print(f'cannot new table : {tablename} in Schema : {schema} because {e}')
        connect.close()

#create_table('usstock_hourly','usstock')
######### add index to Table  ################################################
def add_index2table(tablename:str,schema:str):
    connect, cursor = connect_db(schema=schema)
    if cursor is None:
        print('cursor object is None, please check connection')
    else:
        sql = f"ALTER TABLE {tablename} ADD INDEX (ticker, timestamp); "
        with connect.cursor() as cursor:
            try:
                cursor.execute(sql)
                connect.commit()
                print(f'adding index to table : {tablename} in Schema {schema}')
            except Exception as e:
                print(f'cannnot index to table : {tablename} in Schema {schema} because {e}')

#add_index2table('usstock_hourly','usstock')

def create_table_with_index(tablename:str,schema:str) :
    create_table(tablename=tablename,schema=schema)
    add_index2table(tablename=tablename,schema=schema)

#create_table_with_index(tablename='hour_data',schema='usstock')
####  Drop TableName ########################################################


def drop_table(tablename, schema:str):
    connect, cursor = connect_db(schema=schema)
    if cursor is None:
        print('cursor object is None, please check connection')
    else:
        sql = f"DROP TABLE IF EXISTS {tablename}"
        with connect.cursor() as cursor:
            try:
                cursor.execute(sql)
                connect.commit()
                print(f"drop Table: {tablename} in Schema : {schema}")
            except Exception as e:
                print(f"cannot drop Table: {tablename} in Schema : {schema} because {e}")
# for tablename in ['alpha_vantage_30min', 'alpha_vantage_60min', 'alpha_vantage_daily']:
#drop_table(tablename='hourly',schema='usstock')

#################################################################################################################################################################################
# freq_list=sources_config.alpha_vantage.freq
# for freq in freq_list:
#     tablename=f'alpha_vantage_{freq}'
#     # add_index_table(tablename)
