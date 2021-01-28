
from config.config import sources_config
import requests
import pandas as pd
import json
#outputsize=compact. Strings compact and full
source_config=sources_config.alpha_vantage


def intraday_data(ticker:str,time_interval:str='60min',outputsize:str='full',slice:str='year1month1'):
    """
    :param function: TIME_SERIES_INTRADAY
    :param time_interval:  1min, 5min, 15min, 30min, 60min
    :param adjusted: true/false
    :param slice:  year1month1, year1month2, year1month3, ..., year1month11, year1month12, year2month1, year2month2, year2month3, ..., year2month11, year2month12
    :return:displays companies records per minute
    """
   # url = f'''https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&amp;symbol='+{ticker} +'&amp;interval={time_interval}&amp;apikey={API_keys.alpha_vantage}&amp;outputsize={outputsize}&amp;datatype=json'''
    url=f'''https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={ticker}&interval={time_interval}&apikey={source_config.API_key}&slice={slice}&outputsize={outputsize}&adjusted=true'''
    fp=requests.get(url)
    # Load the JSON to a Python list & dump it back out as formatted JSON
    columns=fp.text.split('\n')[0].strip().split(',')
    data=[tuple(row.strip().split(',')) for row in fp.text.split('\n')[1:]]
    df = pd.DataFrame(data,columns=columns)
    df=df.dropna()
    df.rename(columns={"time":"timestamp"}, inplace=True)
    df.set_index('timestamp', inplace=True)
    return df



    # json_data=fp.json()
    # parsed_json_data = json_data[f'Time Series ({time_interval})']
    # df = pd.DataFrame.from_dict(parsed_json_data, orient='index')
    # df = df.rename({'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume',})
    # return df
   # return mystr

def daily_data(ticker:str,time_interval:str='60min',outputsize:str='full',slice:str='year1month2'):
    """
    :param function: TIME_SERIES_DAILY_ADJUSTED
    :return:displays companies records per minute
    """
    url=f'''https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={source_config.API_ke}&outputsize={outputsize}&datatype=json'''
    fp = requests.get(url)
    json_data=fp.json()
    parsed_json_data = json_data[f'Time Series ({time_interval})']
    df = pd.DataFrame.from_dict(parsed_json_data, orient='index')
    df = df.rename({'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume',})
    return df

result=intraday_data('AAPL')


