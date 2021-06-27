import time
import datetime
from datetime import datetime
import pandas as pd
import pymongo
import json
import requests
from math import sqrt

IP_BROKER = '160.78.100.132'
IP_MONGO_DB = '160.78.28.56'

def download_finance(ticker, interval, period1, period2 = datetime.now()):
    period1 = int(time.mktime(period1.timetuple()))
    period2 = period2.replace(hour=14, minute=30, second=0, microsecond=0)
    period2 = int(time.mktime(period2.timetuple()))

    query_string = f'https://query1.finance.yahoo.com/v7/finance/chart/{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
    print(query_string)
    response = requests.get(query_string)
    data = json.loads(response.content.decode())
    #print(data)

    if (data['chart']['error'] != None):
        print(ticker + ' ' + data['chart']['error']['code']) # TODO: aggiungere a lista da restituire a master così poi non ne calcola le correlazioni
        return

    #print(data['chart']['result'][0]['indicators']['quote'][0])
    if (data['chart']['result'][0]['indicators']['quote'][0] == {}):
        print(ticker + ' already updated')
        return

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
        datetimeData = pd.to_datetime(i, unit="s")
        timestamp.append(datetimeData)


    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))  #160.78.28.56
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

def get_adj_close(ticker, T):
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB)) #160.78.28.56
    mydb = myclient["MarketDB"]
    mycol = mydb[ticker]
    cursor = mycol.find(
    sort = [( '_id', pymongo.DESCENDING )], 
    limit= T #numero di giorni che vogliamo
    )
    last_doc = list(cursor)
    #print(last_doc)
    adj_close = []
    for j in last_doc:
        adj_close.append(j["AdjClose"])
    return adj_close

def get_correlation(adj_close_1, adj_close_2, T):
    #arg1
    product = [x*y for x,y in zip(adj_close_1,adj_close_2)]
    arg1 = sum(product)/T
    
    #arg2
    r1_brack = sum(adj_close_1)/T
    r2_brack = sum(adj_close_2)/T
    arg2 = r1_brack * r2_brack
    
    r1_quad = [x*y for x,y in zip(adj_close_1,adj_close_1)]
    r2_quad = [x*y for x,y in zip(adj_close_2,adj_close_2)]
    
    r1_quad_sottr = [x - (r1_brack*r1_brack) for x in r1_quad]
    r2_quad_sottr = [x - (r2_brack*r2_brack) for x in r2_quad]
    
    arg3 = sum(r1_quad_sottr)/T
    arg4 = sum(r2_quad_sottr)/T
    
    if (sqrt(arg3*arg4) != 0):
        corr_mantegna = (arg1 -arg2)/sqrt(arg3*arg4)
    else:
        corr_mantegna = 0 #indefinita
    return corr_mantegna
