import pandas as pd
import yfinance as yf
import json 
import pymongo
from datetime import datetime

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["MarketDB"]

stocklist = ['AAPL','GOOG','FB','AMZN','COP'] #lista di ticker ricevuta da coord

#controllo quando ho fatto l'ultimo aggiornamento
mycol = mydb[stocklist[0]]
last_doc = mycol.find_one(
  sort=[( '_id', pymongo.DESCENDING )]
)
print(last_doc)
if (last_doc != None):
    last_date = last_doc["Datetime"]
else:
    last_date = datetime(2021, 5, 10) #impostare default
print(last_date)

#ciclo for per ogni azienda, mando richiesta e salvo su db
data = yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers = stocklist,

        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        #period = "5d",
        start=last_date,  #end def is now
        

        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval = "5m",

        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        group_by = 'ticker',

        # adjust all OHLC automatically
        # (optional, default is False)
        #auto_adjust = True,

        # download pre/post regular market hours data
        # (optional, default is False)
        prepost = True,

        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads = True,

        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        proxy = None
    )

#print(data)

for ticker in stocklist:
    
    mycol = mydb[ticker]
    data_tick = data[ticker]
    #print(data_spy)
    data_tick.reset_index(inplace=True) 
    data_dict = data_tick.to_dict("records") 
    #print(data_dict)
    i = 0
    for i in range(len(data_dict)):
        mycol.insert_one(data_dict[i]) 
