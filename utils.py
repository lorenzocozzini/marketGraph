import time
import datetime
from datetime import datetime
import pymongo
import json
import requests

def download_finance(ticker, interval, period1, period2 = datetime.now()):
    period1 = int(time.mktime(period1.timetuple()))
    period2 = int(time.mktime(period2.timetuple()))

    query_string = f'https://query1.finance.yahoo.com/v7/finance/chart/{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
    print(query_string)
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


    myclient = pymongo.MongoClient("mongodb://160.78.28.56:27017/")  #160.78.28.56
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
