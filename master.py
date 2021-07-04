from numpy.lib import utils
import paho.mqtt.client as mqtt
import pandas as pd
import json
import sys
import multiprocessing
import networkx as nx
import pylab
from utils import get_adj_close, get_correlation, IP_BROKER
import progressbar

done_msg = 0
df = pd.read_csv('us_market.csv')
symbol_array = df["Symbol"].values

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
                    corr_mantegna = get_correlation(adj_close_1, adj_close_2)
                    #bar.update(j+1) #sistemareeee
                    #sleep(0.2)
                    corr_list.append((list[i], list[j], round(corr_mantegna, 3))) 
                else:
                    print(list[j] + " non ha dati sufficienti")
        else:
            print(list[i] + " non ha dati sufficienti")
        return_list[id] = corr_list

        #bar.finish()
    
    return

def on_connect(client, userdata, flags, rc):
        print("Connected to a broker!")
        client.subscribe("Node")
        
def on_message(client, userdata, message):
    msg = message.payload.decode()
    print("Messaggio: "+ msg)
    global done_msg
    global symbol_array
    if (type(msg) == list):  #gestire lista aziende non trovate
        done_msg += 1 
        print(done_msg)
        print(msg)
        l3 = [x for x in symbol_array if x not in msg]#tolgo da lista gli errori
        symbol_array = l3
        

def elab_dati(symbol_array):
    G = nx.Graph()
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
            G.add_edges_from([(tupla[0],tupla[1])], weight=tupla[2])   #TODO: stampare anche su file 
    
    edge_labels=dict([((u,v,),d['weight'])
                for u,v,d in G.edges(data=True)])
    pos=nx.kamada_kawai_layout(G)
    nx.draw_networkx_edge_labels(G,pos,edge_labels = edge_labels)
    nx.draw(G,pos,with_labels = True, node_size=200, node_color = 'lightblue')
    pylab.show()

if __name__ == '__main__':

    message = json.dumps(symbol_array.tolist())

    client = mqtt.Client()
    client.connect(IP_BROKER, 9999)
    client.publish("Symbol", message)  

    n_nodes = int(sys.argv[1])    #py master.py 10 10
    T = int(sys.argv[2]) 

    while (done_msg < n_nodes):
        client.on_connect = on_connect
        client.on_message = on_message
        client.loop_start()    #https://stackoverflow.com/a/62950290
        #in mezzo fare cose che vogliamo
        client.loop_stop()
        
    print("Fine scambio msg")
    
    ''' 
    Elaborazione dati 
    '''
    #il master ha già lista completa symbol_array
    #toglie lista di aziende che i nodi non hanno trovato TODO
    elab_dati(symbol_array)
    
    
    
