#!/usr/bin/python
import pymysql
import logging
from datetime import datetime

from pathlib import Path
import sys
current_path=Path().resolve()
sys.path.append(str(current_path))

from config.Config import db_config,data_source_config
from db_setting import db_func


schema='usstock'#'binance'
tablename='daily_data'


#db_func.create_schema(schemaName=crypto_schema)
#db_func.create_table(tablename=tablename,schema='binance')
#db_func.drop_table(tablename='dayily_data',schema='binance')
db_func.show_db_table(schema=schema)
