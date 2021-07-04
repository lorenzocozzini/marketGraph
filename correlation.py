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

T = 10 #TODO leggi da args
G = nx.Graph()

#definisco fuori???
corr01 = []
corr12 = []
corr23 = []
corr34 = []
corr45 = []
corr56 = []
corr67 = []
corr78 = []
corr89 = []
corr91 = []

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

def worker(list, return_list,corrHist):
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

    bar.start()
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

                    #metto nelle varie liste a seconda del valore di corr: sicuro si può fare migliore
                    if(corr_mantegna > 0.0 and corr_mantegna <= 0.1):
                        corr01.append(corr_mantegna)
                    elif(corr_mantegna > 0.1 and corr_mantegna <= 0.2):
                        corr12.append(corr_mantegna)
                    elif(corr_mantegna > 0.2 and corr_mantegna <= 0.3):
                        corr23.append(corr_mantegna)
                    elif(corr_mantegna > 0.3 and corr_mantegna <= 0.4):
                        corr34.append(corr_mantegna)
                    elif(corr_mantegna > 0.4 and corr_mantegna <= 0.5):
                        corr45.append(corr_mantegna)
                    elif(corr_mantegna > 0.5 and corr_mantegna <= 0.6):
                        corr56.append(corr_mantegna)
                    elif(corr_mantegna > 0.6 and corr_mantegna <= 0.7):
                        corr67.append(corr_mantegna)
                    elif(corr_mantegna > 0.7 and corr_mantegna <= 0.8):
                        corr78.append(corr_mantegna)
                    elif(corr_mantegna > 0.8 and corr_mantegna <= 0.9):
                        corr89.append(corr_mantegna)
                    elif(corr_mantegna > 0.9 and corr_mantegna <= 1.0):
                        corr89.append(corr_mantegna)

                    bar.update(j+1) #sistemareeee
                    sleep(0.2)

                    if (corr_mantegna > 0.75):
                        corr_list.append((list[i], list[j], round(corr_mantegna, 3))) 
                else:
                    print(list[j] + " non ha dati sufficienti")
        else:
            print(list[i] + " non ha dati sufficienti")

        
        #metto tutte le correlazioni nelle varie liste e faccio istogramma
        #plt.bar(corr89, corr89.count)
        #plt.show()
        #calcolo poi la gaussiana e poi la deviazione std: sarà la mia theta quindi dobbiamo cambiare dove mettiamo cose in corr list
        return_list[id] = corr_list

        #passo lista correlazioni per istogramma
        corrHistPass = (len(corr01),len(corr12),len(corr23),len(corr34),len(corr45),len(corr56),len(corr67),len(corr78),len(corr89),len(corr91))
        corrHist[0] = corrHistPass
        bar.finish()
    
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
    corrHist = manager.dict()
   
    for i in range(5):
        #p = multiprocessing.Process(name=str(i),target=worker, args=(symbol_array, corr_list))
        p = multiprocessing.Process(name=str(i),target=worker, args=(sub_list, corr_list, corrHist))
        jobs.append(p)
        p.start()
    #quando tutti i processi hanno finito mostro grafo
    for job in jobs:
        job.join()
    
    print("Fine processing correlation")

    #Ho calcolato tutte le correlazioni: e ho come risultato il dict del numero di correlazioni per ogni range di valori
    #ora faccio istogramma
    indices = np.arange(len(corrHist[0]))
    word = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    frequency = [corrHist[0][0], corrHist[0][1],corrHist[0][2], corrHist[0][3], corrHist[0][4], corrHist[0][5], corrHist[0][6], corrHist[0][7], corrHist[0][8], corrHist[0][9]]
    mu, std = norm.fit(frequency) 
    plt.bar(indices, frequency, color='r')
    plt.xticks(indices, word, rotation='vertical')
    plt.tight_layout()
    #plt.show()
    #?????non va bene la mu ma non so perchè
    #plt.hist(frequency, bins=25, density=True, alpha=0.4, color='g') #alpha è trasparenza
    xmin, xmax = plt.xlim()
    x = np.linspace(xmin, xmax, 100)
    p = norm.pdf(x, mu, std)
    plt.plot(x, p, 'k', linewidth=2)
    title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
    plt.title(title)
    plt.show()


    #Creo il file
    import csv
    with open('correlation.csv', mode='w', newline='') as csv_file:
        colonne = ['Source', 'Target', 'Weight']
        writer = csv.DictWriter(csv_file, fieldnames=colonne)
        writer.writeheader()
        
    #disegno grafo e scrivo su CSV
        for i in range(5):
            for tupla in corr_list[i]:
                #G.add_edges_from([(tupla[0],tupla[1])], weight=tupla[2])
                writer.writerow({'Source': tupla[0], 'Target': tupla[1], 'Weight': tupla[2]})
    
    edge_labels=dict([((u,v,),d['weight'])
                for u,v,d in G.edges(data=True)])
    #pos=nx.kamada_kawai_layout(G)
    #nx.draw_networkx_edge_labels(G,pos,edge_labels = edge_labels)
    #nx.draw(G,pos,with_labels = True, node_size=200, node_color = 'lightblue')
    #pylab.show()
