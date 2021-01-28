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


crypto_schema='binance'
crypto_tablename='hour_data'


#db_func.create_schema(schemaName=crypto_schema)
db_func.create_table(tablename=crypto_tablename,schema=crypto_schema)
