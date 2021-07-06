import paho.mqtt.client as mqtt
import json
import sys
import csv
import multiprocessing
from utils import get_adj_close, get_symbol_array, get_correlation, start_timer, increase_timer, same_date, get_threshold, get_edges, IP_BROKER


done_msg = 0
timeout = 3600000000000 #un'ora, tempo massimo di attesa della risposta dei nodi
symbol_array = get_symbol_array()
#symbol_array = symbol_array[4000:4400]
T = int(sys.argv[2]) 
N_WORKER = 5
 
def worker(list, return_list):
    id = int(multiprocessing.current_process().name)
    num_records = int(len(list)/N_WORKER)
    corr_list = []
    # Assegnazione delle aziende ai singoli worker per il calcolo delle correlazioni
    if (id + 1 < N_WORKER):
        last_index = num_records*(id+1)
    # L'ultimo worker prende tutti i ticker rimasti
    else:
         last_index = len(list)
    
    # Per ogni azienda assegnata al worker, si calcolano le correlazioni con tutte le altre aziende (solo quelle successive nella lista dal momento che è commutativa)
    for i in range(num_records*id, last_index):
        datetime_adj_close_1 = get_adj_close(list[i], T)
        adj_close_1 = []
        for k in range(len(datetime_adj_close_1)):
            adj_close_1.append(datetime_adj_close_1[k][1])
            
        # E' possibile calcolare la correlazione solo se sono presenti dati per tutto l'intervallo e non ci sono dati null
        if (len(adj_close_1) == T and not(None in adj_close_1)):
            for j in range(i+1, len(list)):
                
                datetime_adj_close_2 = get_adj_close(list[j], T)
                adj_close_2 = []
                for h in range(len(datetime_adj_close_2)):
                    adj_close_2.append(datetime_adj_close_2[h][1])
                    
                # E' possibile calcolare la correlazione solo se sono presenti dati per tutto l'intervallo e non ci sono dati null
                if (len(adj_close_2) == T and not(None in adj_close_2)):
                    # E' possibile calcolare la correlazione solo se si stanno considerando i dati per le stesse giornate
                    if (same_date(datetime_adj_close_1, datetime_adj_close_2)):
                        # Calcolo correlazione
                        correlation = get_correlation(adj_close_1, adj_close_2, T)
                        # Aggiungo correlazione alla lista di correlazioni da restituire al master
                        corr_list.append((list[i], list[j], round(correlation, 3))) 
                    else:
                        print("Non matching dates, impossible to get correlation for {} - {}".format(list[i], list[j]))
                else:
                    print(list[j] + " does not have enough data")
        else:
            print(list[i] + " does not have enough data")
        return_list[id] = corr_list

def on_connect(client, userdata, flags, rc):
        print("Connected to a broker!")
        client.subscribe("Node")
        
def on_message(client, userdata, message):
    msg = message.payload.decode()
    
    print("Message: "+ msg)
    global done_msg
    global symbol_array
    # Gestione delle aziende non trovate
    if (msg != '[]'):  
        
        msg = msg[2:-2]
        list_msg = msg.split('", "')
        # Si rimuove dalla lista di aziende quelle che non sono state trovate
        updated_list = [x for x in symbol_array if x not in list_msg]
        symbol_array = updated_list
        print(symbol_array)
    done_msg += 1 
        

def elab_dati(symbol_array):
    jobs = []
    manager = multiprocessing.Manager()
    correlation_list = manager.dict()
    print("Start processing correlation")
    # Correlazioni calcolate tramite multiprocessing
    for i in range(N_WORKER):
        p = multiprocessing.Process(name=str(i),target=worker, args=(symbol_array, correlation_list))
        jobs.append(p)
        p.start()
    
    # Attesa per il completamento del calcolo delle correlazione da parte di tutti i worker
    for job in jobs:
        job.join()
    
    print("End processing correlation")

    # Calcolo della soglia per la correlazione
    theta = get_threshold(correlation_list)
    # Tra tutte le correlazioni, si mantengono quelle con correlazione non inferiore alla soglia
    edges_list = get_edges(theta, correlation_list)


    # Salvataggio su file dei nodi (Source, Target) e degli archi (Weight) tra essi
    with open('correlation.csv', mode='w', newline='') as csv_file:
        colonne = ['Source', 'Target', 'Weight']
        writer = csv.DictWriter(csv_file, fieldnames=colonne)
        writer.writeheader()
        
        for tupla in edges_list:
            writer.writerow({'Source': tupla[0], 'Target': tupla[1], 'Weight': tupla[2]})

if __name__ == '__main__':

    message = json.dumps(symbol_array) 

    client = mqtt.Client()
    client.connect(IP_BROKER, 9999)
    client.publish("Symbol", message)

    n_nodes = int(sys.argv[1])
   
    interval = start_timer()
    while (done_msg < n_nodes):
        client.on_connect = on_connect
        client.on_message = on_message
        #https://stackoverflow.com/a/62950290
        client.loop_start()    
        interval = increase_timer(interval)
        if (interval > timeout):
            print("Node in fail, market graph may not be complete")
            break
        client.loop_stop()

    
    print("Message exchange terminated")
    interval = start_timer()
    elab_dati(symbol_array)
    interval = increase_timer(interval)
    print("Calculation of correlation completed in " + str(interval/1000) + " seconds")
