from numpy.core.records import array
from numpy.lib import utils
import paho.mqtt.client as mqtt
import pandas as pd
import json
import sys
import multiprocessing
import networkx as nx
import pylab
from utils import get_adj_close, get_symbol_array, get_correlation, start_timer, increase_timer, same_date, get_hist, get_edges, delete_duplicates, IP_BROKER
import progressbar

done_msg = 0
timeout = 3600000000000 #un'ora 

symbol_array = get_symbol_array()
#symbol_array = symbol_array[4000:4100] #TODO TOGLI
T = int(sys.argv[2]) 
 
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

    #bar.start()
    for i in range(num_records*id, last_index):
        #print("Ticker 1: " + list[i])
        adj_close_1 = get_adj_close(list[i], T)
        #print("adj_close_1", adj_close_1)
        if (len(adj_close_1) == T and not(None in adj_close_1)):                 #calcolo correlazione solo se ho i dati per tutto l'intervallo e non ci sono dati null
            global G
            for j in range(i+1, len(list)):
                #print("Ticker 2: " + list[j])
                
                adj_close_2 = []
                adj_close_2 = get_adj_close(list[j], T)
                #print("adj_close_2 " + list[j], adj_close_2)
                if (len(adj_close_2) == T and not(None in adj_close_2)):          #calcolo correlazione solo se ho i dati per tutto l'intervallo e non ci sono dati null
                                                                                  #mettere controllo per corrispondenza giorni???
                    if (same_date(adj_close_1, adj_close_2)):
                        #calcola correlazione
                        corr_mantegna = get_correlation(adj_close_1, adj_close_2, T)
                        #bar.update(j+1) #sistemareeee
                        #sleep(0.2)
                        corr_list.append((list[i], list[j], round(corr_mantegna, 3))) 
                    else:
                        print("Le date dell'intervallo desiderato non corrispondono")
                else:
                    print(list[j] + " non ha dati sufficienti")
        else:
            print(list[i] + " non ha dati sufficienti")
        return_list[id] = corr_list    
    return

def on_connect(client, userdata, flags, rc):
        print("Connected to a broker!")
        client.subscribe("Node")
        
def on_message(client, userdata, message):
    msg = message.payload.decode()
    
    print("Messaggio: "+ msg)
    global done_msg
    global symbol_array
    if (msg != '[]'):  #gestire lista aziende non trovate
        done_msg += 1 
        msg = msg[2:-2]
        list_msg = msg.split('", "')
        l3 = [x for x in symbol_array if x not in list_msg]#tolgo da lista gli errori
        symbol_array = l3
        print(symbol_array)
        

def elab_dati(symbol_array):
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

    theta = get_hist(corr_list)
    edges_list= get_edges(theta, corr_list)

    #Creo il file
    import csv
    with open('correlation.csv', mode='w', newline='') as csv_file:
        colonne = ['Source', 'Target', 'Weight']
        writer = csv.DictWriter(csv_file, fieldnames=colonne)
        writer.writeheader()
        
        for tupla in edges_list:
            writer.writerow({'Source': tupla[0], 'Target': tupla[1], 'Weight': tupla[2]})

if __name__ == '__main__':

    message = json.dumps(symbol_array) #.to_list()

    client = mqtt.Client()
    client.connect(IP_BROKER, 9999)
    client.publish("Symbol", message)  

    n_nodes = int(sys.argv[1])    #py master.py 10 10
   
    interval = start_timer()
    while (done_msg < n_nodes):
        client.on_connect = on_connect
        client.on_message = on_message
        client.loop_start()    #https://stackoverflow.com/a/62950290
        #in mezzo fare cose che vogliamo
        #print(done_msg)
        interval = increase_timer(interval)
        if (interval > timeout):
            print("Un nodo è in fail, il calcolo delle correlazioni non sarà completo")
            break
        client.loop_stop()

    
    print("Fine scambio msg")
    interval = start_timer()
    elab_dati(symbol_array)
    interval = increase_timer(interval)
    print("Tempo impiegato per l'elaborazione dei dati: " + str(interval/1000) + "secondi")

    
    
