#calcolo correlazione
import multiprocessing
import pandas as pd
import json
import pymongo
import numpy as np
import utils
from datetime import datetime 
import networkx as nx
import matplotlib.pyplot as plt
import pylab

from math import sqrt

T = 10 #TODO leggi da args
G = nx.DiGraph()

def get_adj_close(ticker):
    myclient = pymongo.MongoClient("mongodb://160.78.28.56:27017/") #160.78.28.56
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


def get_correlation(adj_close_1, adj_close_2):
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
    
    corr_mantegna = (arg1 -arg2)/sqrt(arg3*arg4)
    return corr_mantegna

def worker(list, return_list):
    id = int(multiprocessing.current_process().name)
    print(list)
    num_records = int(len(list)/5)
    corr_list = []
    #scarica i dati e calcola correlazioni
    for i in range(num_records*id, num_records*(id+1)):
        print("Ticker 1: " + list[i])
        adj_close_1 = get_adj_close(list[i])
        print("adj_close_1", adj_close_1)
        if (len(adj_close_1) == T):                 #calcolo correlazione solo se ho i dati per tutto l'intervallo
            global G
            for j in range(i+1, len(list)):
                print("Ticker 2: " + list[j])
                adj_close_2 = []
                adj_close_2 = get_adj_close(list[j])
                print("adj_close_2", adj_close_2)
                if (len(adj_close_2) == T):          #calcolo correlazione solo se ho i dati per tutto l'intervallo
                    #calcola correlazione
                    print("Correlation " + list[i] + " - " + list[j] + ":")
                    correlation = np.corrcoef(adj_close_1, adj_close_2)
                    print(correlation)
                    
                    corr_mantegna = get_correlation(adj_close_1, adj_close_2)
                    print("Correlation " + list[i] + " - " + list[j] + ":")
                    print(str(corr_mantegna))
                    
                    #if -> mostro solo se correlazione > theta
                    #G.add_edges_from([(list[i], list[j])], weight=corr_mantegna)
                    if (corr_mantegna > 0.75):
                        corr_list.append((list[i], list[j], corr_mantegna))
                    
                else:
                    print("adj_close_2 non ha dati sufficienti")
        else:
            print("adj_close_1 non ha dati sufficienti")
        return_list[id] = corr_list
    
            
    #print (multiprocessing.current_process().name," Worker")
    return

if __name__ == '__main__':
    df = pd.read_csv('us_market.csv')
    symbol_array = df["Symbol"].values
    #message = json.dumps(symbol_array.tolist())
    adj_close = []
    sub_list = symbol_array[0:16]
    #print(sub_list)
    jobs = []
    manager = multiprocessing.Manager()
    corr_list = manager.dict()
    for i in range(5):
        p = multiprocessing.Process(name=str(i),target=worker, args=(sub_list, corr_list))
        jobs.append(p)
        p.start()
    
    #quando tutti i processi hanno finito mostro grafo
    for job in jobs:
        job.join()
    
    print("Tutto finito")
    #print(corr_list)
    #G.add_edges_from([('C','F')], weight=0.214214)
    for i in range(5):
        for tupla in corr_list[i]:
            G.add_edges_from([(tupla[0],tupla[1])], weight=tupla[2])
    
    
    options = {
        'node_color': 'red',
        'node_size': 1000,
        'width': 3,
        'arrowstyle': '-',
        'arrowsize': 0.2,
    }
    nx.draw_networkx(G, arrows=True, **options)
    pylab.show()
