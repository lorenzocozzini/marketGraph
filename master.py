import paho.mqtt.client as mqtt
import json
import sys
import csv
from datetime import timedelta
from utils import get_adj_close, get_symbol_array, start_timer, increase_timer, get_threshold, get_edges, IP_BROKER, DOWNLOAD_TYPE, EVALUATION_TYPE
import progressbar
from time import sleep

done_msg = 0
timeout = 9000 #due ore e mezzo, tempo massimo di attesa della risposta dei nodi
symbol_array = []
symbol_array.append('%5EGSPC')
#symbol_array = symbol_array[4000:4050]
T = int(sys.argv[2]) 
correlation_list = []

def on_connect(client, userdata, flags, rc):
        print("Connected to a broker!")
        client.subscribe("Node")
        
def on_download_message(client, userdata, message):
    message = json.loads(message.payload.decode())
    #print("Message: "+ str(message))
    global done_msg
    global symbol_array
    # Gestione delle aziende non trovate
    list_msg = message['error_list']
    if (list_msg != []):  
        # Si rimuove dalla lista di aziende quelle che non sono state trovate
        updated_list = [x for x in symbol_array if x not in list_msg]
        symbol_array = updated_list
        #print(symbol_array)
    done_msg += 1
    
def on_evaluation_message(client, userdata, message):
    message = json.loads(message.payload.decode())
    #print("Message: "+ str(message))
    global done_msg
    global correlation_list
    corr_list = message['correlation_list']
    # Gestione delle aziende non trovate
    if (corr_list != []):  
        correlation_list.extend(corr_list)
    done_msg += 1

def elab_dati(correlation_list):
    # Calcolo della soglia per la correlazione
    print("Finding threshold...")
    theta = get_threshold(correlation_list)
    
    # Tra tutte le correlazioni, si mantengono quelle con correlazione non inferiore alla soglia
    print("Getting edges...")
    edges_list = get_edges(theta, correlation_list)

    # Salvataggio su file dei nodi (Source, Target) e degli archi (Weight) tra essi
    with open('correlation.csv', mode='w', newline='') as csv_file:
        colonne = ['Source', 'Target', 'Weight']
        writer = csv.DictWriter(csv_file, fieldnames=colonne)
        writer.writeheader()
        
        for tupla in edges_list:
            writer.writerow({'Source': tupla[0], 'Target': tupla[1], 'Weight': tupla[2]})

if __name__ == '__main__':

    message = {
        "type" : DOWNLOAD_TYPE,
        "array" :  symbol_array
    }
    
    #print (message)
    
    client = mqtt.Client()
    client.connect(IP_BROKER, 9999, keepalive=7200)
    client.publish("Master", json.dumps(message))

    n_nodes = int(sys.argv[1])
   
    interval = start_timer()
    while (done_msg < n_nodes):
        client.on_connect = on_connect
        client.on_message = on_download_message
        #https://stackoverflow.com/a/62950290
        client.loop_start()    
        interval = increase_timer(interval)
        if (interval > timeout):
            print("Node in fail, market graph may not be complete")
            break
        client.loop_stop() 
        
    print("Message exchange terminated")
    interval = increase_timer(interval)
    print("Elapsed time for downloading " + str(timedelta(seconds=interval)))
    
    interval = start_timer()
    
    # Prova #
    # Inizializzazione della progressbar per feedback grafico dell'andamento dei download
    print("Start downloading adjusted close for period of interest (last {} days)".format(T))
    bar = progressbar.ProgressBar(maxval=len(symbol_array), \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    i = 0
    bar.start()
    data_array = []
    for ticker in symbol_array:
        datetime, adj_close = get_adj_close(ticker, T)
        if (len(datetime) == T and not(None in adj_close)):
            data_array.append({
                "ticker" : ticker, 
                "datetime" : datetime,
                "adj_close" : adj_close
                })
        bar.update(i)
        sleep(0.2)
        i += 1
    
    bar.finish()
    
    message = {
        "type" : EVALUATION_TYPE,
        "array" : data_array,
        "T" : T
    }
    
    client.publish("Master", json.dumps(message))
    
    print("Start processing correlation")
    done_msg = 0
    while (done_msg < n_nodes):
        client.on_connect = on_connect
        client.on_message = on_evaluation_message
        #https://stackoverflow.com/a/62950290
        client.loop_start()
        interval = increase_timer(interval)
        if (interval > timeout):
            print("Node in fail, market graph may not be complete")
            break
        client.loop_stop() 
    
    client.disconnect()
    
    elab_dati(correlation_list)
    interval = increase_timer(interval)
    print("Elapsed time for correlation " + str(timedelta(seconds=interval)))
