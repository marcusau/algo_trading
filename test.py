import yaml
from pathlib import Path

d = Path().resolve().parent
print(Path().resolve())
# def get_value(ticker,time_interval):
#     js = import_web(ticker,time_interval)
#     parsed_data = json.loads(js) # loads the json and converts the json string into dictionary
#     ps = parsed_data[f'Time Series ({time_interval})']
#
