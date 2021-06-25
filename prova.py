import time
import datetime
import pandas as pd
from datetime import datetime
import pymongo

ticker = 'ADOC'
period1 = int(time.mktime(datetime.datetime(2020, 12, 1, 23, 59).timetuple()))
period2 = int(time.mktime(datetime.datetime(2020, 12, 31, 23, 59).timetuple()))
interval = '1d' # 1d, 1m
import json
import requests
query_string = f'https://query1.finance.yahoo.com/v7/finance/chart/{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
response = requests.get(query_string)
data = json.loads(response.content.decode())
print(data)


open = data['chart']['result'][0]['indicators']['quote'][0]['open']
high = data['chart']['result'][0]['indicators']['quote'][0]['high']
low = data['chart']['result'][0]['indicators']['quote'][0]['low']
close = data['chart']['result'][0]['indicators']['quote'][0]['close']
adj_close = data['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
volume = data['chart']['result'][0]['indicators']['quote'][0]['volume']

timestamp = []
for i in data['chart']['result'][0]['timestamp']:
    data = datetime.fromtimestamp(i)
    data = data.strftime("%Y-%m-%dT%H:%M:%S")
    timestamp.append(data)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["MarketDB"]
mycol = mydb[ticker]

for i in range(len(timestamp)):
    object = {"Datetime": timestamp[i],
            "Open": open[i],
            "High": high[i],
            "Low": low[i],
            "Close": close[i],
            "AdjClose": adj_close[i],
            "Volume": volume[i]
            }
    mycol.insert_one(object)
