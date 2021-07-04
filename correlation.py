#calcolo correlazione
import multiprocessing
from typing import Sequence
import pandas as pd
import json
import pymongo
import numpy as np
import utils
from datetime import datetime 
import networkx as nx
import matplotlib.pyplot as plt
import pylab
import matplotlib.mlab as mlab
from scipy.stats import norm
from time import sleep
import progressbar
from math import sqrt
import scipy.stats as st

T = 10 #TODO leggi da args
G = nx.Graph()

def get_hist(corr_list):
    
    correlation_count = [0] * 10

     #metto nelle varie liste a seconda del valore di corr: sicuro si può fare migliore

    #print(corr_list)
    correlation_value = []
    
    for i in range(len(corr_list)):
        for tupla in corr_list[i]:
            correlation_value.append(tupla[2])
            if(tupla[2] > 0.0 and tupla[2] <= 0.1):
                correlation_count[0]+=1
            elif(tupla[2] > 0.1 and tupla[2] <= 0.2):
                correlation_count[1]+=1
            elif(tupla[2] > 0.2 and tupla[2] <= 0.3):
                correlation_count[2]+=1
            elif(tupla[2] > 0.3 and tupla[2] <= 0.4):
                correlation_count[3]+=1
            elif(tupla[2] > 0.4 and tupla[2] <= 0.5):
                correlation_count[4]+=1
            elif(tupla[2] > 0.5 and tupla[2] <= 0.6):
                correlation_count[5]+=1
            elif(tupla[2] > 0.6 and tupla[2] <= 0.7):
                correlation_count[6]+=1
                #print(tupla)
            elif(tupla[2] > 0.7 and tupla[2] <= 0.8):
                correlation_count[7]+=1
            elif(tupla[2] > 0.8 and tupla[2] <= 0.9):
                correlation_count[8]+=1
            elif(tupla[2] > 0.9 and tupla[2] <= 1.0):
                correlation_count[9]+=1
                #print(tupla)
    
    
    print(correlation_count)
    print(correlation_value)
    
    #disegno istogramma
    indices = np.arange(len(correlation_count))
    word = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    plt.bar(indices, correlation_count, color='r')
    plt.xticks(indices, word, rotation='vertical')
    plt.tight_layout()
    plt.show()

    #calcolo Gaussiana
    mu, std = norm.fit(correlation_value) 
    plt.hist(correlation_value, density=True, bins=200, label="Data")
    mn, mx = plt.xlim()
    plt.xlim(mn, mx)
    kde_xs = np.linspace(mn, mx, 300)
    kde = st.gaussian_kde(correlation_value)
    plt.plot(kde_xs, kde.pdf(kde_xs), label="PDF")
    plt.legend(loc="upper left")
    plt.ylabel('Probability')
    plt.xlabel('Data')
    title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
    plt.title(title)
    plt.show()

    r1 = np.mean(correlation_value)
    print("\nMean: ", r1)
    r2 = np.std(correlation_value)
    print("\nstd: ", r2)
    r3 = np.var(correlation_value)
    print("\nvariance: ", r3)

    return std

def get_edges(theta, corr_list):
    edges_list=[]
    for i in range(len(corr_list)):
        for tupla in corr_list[i]:
            if (tupla[2]>=theta):
                edges_list.append(tupla)
    return edges_list
    
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
    bar = progressbar.ProgressBar(maxval=20, \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    #print(list)
    num_records = int(len(list)/5)
    corr_list = []
    corrHistPass = []
    #se non è l'ultimo 
    if (id + 1 < 5):
        last_index = num_records*(id+1)
    #se è l'ultimo
    else:
         last_index = len(list)
    #scarica i dati e calcola correlazioni

    #bar.start()
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
                    ###########################30 06 2021 ##############################
                    #per fare la Gaussiana qui farei tante liste per ogni range di valori e andrei a mettere quei valori nelle liste:
                    #non ci servono i nomi delle aziende perchè dobbiamo solo trovare il valore di theta

                    #bar.update(j+1) #sistemareeee
                    #sleep(0.2)

                    #Va fuori
                    #if (corr_mantegna > 0.75):
                    corr_list.append((list[i], list[j], round(corr_mantegna, 3))) 
                else:
                    print(list[j] + " non ha dati sufficienti")
        else:
            print(list[i] + " non ha dati sufficienti")
        return_list[id] = corr_list

        #bar.finish()
    
    return

if __name__ == '__main__':
    df = pd.read_csv('us_market.csv')
    symbol_array = df["Symbol"].values  
    
    #il master ha già lista completa
    #toglie lista di aziende che i nodi non hanno trovato
    adj_close = []
    sub_list = symbol_array[0:16]
    jobs = []
    manager = multiprocessing.Manager()
    corr_list = manager.dict()
    #corrHist = manager.dict()
   
    for i in range(5):
        #p = multiprocessing.Process(name=str(i),target=worker, args=(symbol_array, corr_list))
        p = multiprocessing.Process(name=str(i),target=worker, args=(sub_list, corr_list))
        jobs.append(p)
        p.start()
    #quando tutti i processi hanno finito mostro grafo
    for job in jobs:
        job.join()
    
    print("Fine processing correlation")

    theta = get_hist(corr_list)

    edges_list= get_edges(theta, corr_list)


    #Creo il file
    import csv
    with open('correlation.csv', mode='w', newline='') as csv_file:
        colonne = ['Source', 'Target', 'Weight']
        writer = csv.DictWriter(csv_file, fieldnames=colonne)
        writer.writeheader()
        
    #disegno grafo e scrivo su CSV
        #for i in range(5):
        for tupla in edges_list:
            G.add_edges_from([(tupla[0],tupla[1])], weight=tupla[2])
            writer.writerow({'Source': tupla[0], 'Target': tupla[1], 'Weight': tupla[2]})
    
    edge_labels=dict([((u,v,),d['weight'])
                for u,v,d in G.edges(data=True)])
    pos=nx.kamada_kawai_layout(G)
    nx.draw_networkx_edge_labels(G,pos,edge_labels = edge_labels)
    nx.draw(G,pos,with_labels = True, node_size=200, node_color = 'lightblue')
    pylab.show()
