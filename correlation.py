#calcolo correlazione
import multiprocessing
import pandas as pd
import json
import pymongo
import numpy as np

def worker():
    df = pd.read_csv('us_market.csv')
    symbol_array = df["Symbol"].values
    message = json.dumps(symbol_array.tolist())
    id = int(multiprocessing.current_process().name)
    sub_list = message[0:16]
    myclient = pymongo.MongoClient("mongodb://160.78.28.56:27017/") #160.78.28.56
    mydb = myclient["MarketDB"]
    #scarica i dati 
    #fare array
    adj_close = []
    for ticker in sub_list:
        mycol = mydb[ticker]
        cursor = mycol.find(
        sort = [( '_id', pymongo.DESCENDING )], limit= 10 #numero di giorni che vogliamo
        )
        last_doc = list(cursor)
        print(last_doc)
        #adj_close = last_doc["AdjClose"]
        #print(adj_close) 

    


    #calcola correlazione
    #correlation = np.corrcoef(sub_list, adj_close)
    print (multiprocessing.current_process().name," Worker")
    return

if __name__ == '__main__':
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(name=str(i),target=worker)
        jobs.append(p)
        p.start()
