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
G = nx.Graph()

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
    
    if (sqrt(arg3*arg4) != 0):
        corr_mantegna = (arg1 -arg2)/sqrt(arg3*arg4)
    else:
        corr_mantegna = 0 #indefinita
    return corr_mantegna

def worker(list, return_list):
    id = int(multiprocessing.current_process().name)
    #print(list)
    num_records = int(len(list)/5)
    corr_list = []
    #se non è l'ultimo 
    if (id + 1 < 5):
        last_index = num_records*(id+1)
    #se è l'ultimo
    else:
         last_index = len(list)
    #scarica i dati e calcola correlazioni
    for i in range(num_records*id, last_index):
        #print("Ticker 1: " + list[i])
        adj_close_1 = get_adj_close(list[i])
        #print("adj_close_1", adj_close_1)
        if (len(adj_close_1) == T and not(None in adj_close_1)):                 #calcolo correlazione solo se ho i dati per tutto l'intervallo e non ci sono dati null
            global G
            for j in range(i+1, len(list)):
                #print("Ticker 2: " + list[j])
                adj_close_2 = []
                adj_close_2 = get_adj_close(list[j])
                #print("adj_close_2 " + list[j], adj_close_2)
                if (len(adj_close_2) == T and not(None in adj_close_2)):          #calcolo correlazione solo se ho i dati per tutto l'intervallo e non ci sono dati null
                    #calcola correlazione
                    #print("Correlation " + list[i] + " - " + list[j] + ":")
                    #correlation = np.corrcoef(adj_close_1, adj_close_2)
                    #print(correlation)
                    
                    corr_mantegna = get_correlation(adj_close_1, adj_close_2)
                    #print("Correlation " + list[i] + " - " + list[j] + ":")
                    #print(str(corr_mantegna))
                    
                    #if -> mostro solo se correlazione > theta
                    #G.add_edges_from([(list[i], list[j])], weight=corr_mantegna)
                    if (corr_mantegna > 0.75):
                        corr_list.append((list[i], list[j], round(corr_mantegna, 3)))
                    
                else:
                    print(list[j] + " non ha dati sufficienti")
        else:
            print(list[i] + " non ha dati sufficienti")
        return_list[id] = corr_list
    return

if __name__ == '__main__':
    df = pd.read_csv('us_market.csv')
    symbol_array = df["Symbol"].values  
    
    #il master ha già lista completa
    #toglie lista di aziende che i nodi non hanno trovato
    
    
    adj_close = []
    #sub_list = symbol_array[0:16]
    jobs = []
    manager = multiprocessing.Manager()
    corr_list = manager.dict()
    for i in range(5):
        p = multiprocessing.Process(name=str(i),target=worker, args=(symbol_array, corr_list))
        jobs.append(p)
        p.start()
    
    #quando tutti i processi hanno finito mostro grafo
    for job in jobs:
        job.join()
    
    print("Fine processing correlation")
    #disegno grafo
    for i in range(5):
        for tupla in corr_list[i]:
            G.add_edges_from([(tupla[0],tupla[1])], weight=tupla[2])
    
    edge_labels=dict([((u,v,),d['weight'])
                for u,v,d in G.edges(data=True)])
    pos=nx.kamada_kawai_layout(G)
    nx.draw_networkx_edge_labels(G,pos,edge_labels = edge_labels)
    nx.draw(G,pos,with_labels = True, node_size=200, node_color = 'lightblue')
    pylab.show()
