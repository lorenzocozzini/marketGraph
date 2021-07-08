import paho.mqtt.client as mqtt
import sys
import pymongo
from datetime import datetime
from utils import IP_BROKER, IP_MONGO_DB, download_finance, DOWNLOAD_TYPE, EVALUATION_TYPE, get_adj_close, get_correlation, same_date
import json
from math import modf
import progressbar
from time import sleep

download_ended = False
id = int(sys.argv[1])
num_nodes = int(sys.argv[2])

def get_tickers(list_msg):

    len_list = len(list_msg)
    fraz, num_records = modf(len_list/num_nodes)
    num_records = int(num_records)
    # Assegnazione delle aziende ai singoli nodi per il download dei dati aggiornati
    if (id + 1 < num_nodes):
        last_index = num_records*(id + 1)
    # L'ultimo nodo prende tutti i ticker rimasti
    else:
         last_index = len_list
    sub_list = list_msg[id*num_records:last_index] 
    return sub_list

def update_data(stocklist):
    error_list=[]
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))
    mydb = myclient["MarketDB"]
    
    # Inizializzazione della progressbar per feedback grafico dell'andamento dei download
    bar = progressbar.ProgressBar(maxval=len(stocklist), \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    i = 0
    bar.start()
    
    # Per ogni ticker, il nodo verifica la data degli ultimi dati inseriti in DB e inizia a scaricare da quel giorno
    for ticker in stocklist:
        print(ticker)
        mycol = mydb[ticker]
        last_doc = mycol.find_one(
            sort=[( 'Datetime', pymongo.DESCENDING )]
        )
        if (last_doc != None):
            last_date = last_doc["Datetime"]
        # Se non sono presenti dati in DB, si inizia a scaricare dal 2000
        else:
            last_date = datetime(2000, 1, 1)
        
        # Scaricamento dei dati, in caso di errore, si aggiunge il ticker a una lista da restituire al master
        if(download_finance(ticker=ticker, interval='1d', period1=last_date) == -1):
            error_list.append(ticker)
            
        bar.update(i)
        sleep(0.2)
        i += 1
    
    bar.finish()
    return error_list


def evaluate_correlation(list, T):
    num_records = int(len(list)/num_nodes)
    corr_list = []
    # Assegnazione delle aziende ai singoli worker per il calcolo delle correlazioni
    if (id + 1 < num_nodes):
        last_index = num_records*(id+1)
    # L'ultimo worker prende tutti i ticker rimasti
    else:
         last_index = len(list)
    
    skip_ticker = []
    # Inizializzazione della progressbar per feedback grafico dell'andamento dei download
    bar = progressbar.ProgressBar(maxval=last_index-num_records*id, \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    b = 0
    bar.start()
    
    # Per ogni azienda assegnata al worker, si calcolano le correlazioni con tutte le altre aziende (solo quelle successive nella lista dal momento che è commutativa)
    for i in range(num_records*id, last_index):
        ticker_1 = list[i]['ticker']
        adj_close_1 = list[i]['adj_close']
        datetime_1 = list[i]['datetime']
        print(ticker_1)
        #datetime_adj_close_1 = get_adj_close(list[i], T)
        #datetime_adj_close_1 = list[i]['adj_close']
        #adj_close_1 = []
        #for k in range(len(datetime_adj_close_1)):
            #adj_close_1.append(datetime_adj_close_1[k][1])
            
        # E' possibile calcolare la correlazione solo se sono presenti dati per tutto l'intervallo e non ci sono dati null
        if (len(adj_close_1) == T and not(None in adj_close_1)):
            for j in range(i+1, len(list)):
                
                ticker_2 = list[j]['ticker']
                adj_close_2 = list[j]['adj_close']
                datetime_2 = list[j]['datetime']
                
                if not(ticker_2 in skip_ticker): #così evito un po' di chiamate a db
                    #datetime_adj_close_2 = get_adj_close(list[j], T)
                    #datetime_adj_close_2 = list[j]
                    #for h in range(len(datetime_adj_close_2)):
                        #adj_close_2.append(datetime_adj_close_2[h][1])
                        
                    # E' possibile calcolare la correlazione solo se sono presenti dati per tutto l'intervallo e non ci sono dati null
                    if (len(adj_close_2) == T and not(None in adj_close_2)):
                        # E' possibile calcolare la correlazione solo se si stanno considerando i dati per le stesse giornate
                        if (datetime_1 == datetime_2):
                            # Calcolo correlazione
                            correlation = get_correlation(adj_close_1, adj_close_2, T)
                            # Aggiungo correlazione alla lista di correlazioni da restituire al master
                            corr_list.append((ticker_1, ticker_2, round(correlation, 3))) 
                        else:
                            print("Non matching dates, impossible to get correlation for {} - {}".format(list[i][2], list[j][2]))
                    else:
                        print(ticker_2 + " does not have enough data")
                        skip_ticker.append(ticker_2)
        else:
            print(ticker_1 + " does not have enough data")
            
        #return_list[id] = corr_list
        #print("Processo {}, elaborati {}/{}".format(id, i-(num_records*id), last_index-(num_records*id)))
        bar.update(b)
        sleep(0.05)
        b += 1
    
    bar.finish()
    return corr_list

client = mqtt.Client(client_id=str(id))
client.connect(IP_BROKER, 9999, keepalive=7200)

print("Node id: " + str(id))

def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Symbol")

def on_message(client, userdata, message):
    # Alla ricezione di un messaggio, il nodo legge la lista e estrae le aziende a lui assegnate 
    message = json.loads(message.payload.decode())
    print("Message received")
    
    if (message['type'] == DOWNLOAD_TYPE):
        symbol_list = get_tickers(message['array']) 
        # Poi scarica i dati, tenendo traccia delle aziende per cui ha avuto problemi di download
        error_list = update_data(symbol_list)
        # Al termine, il nodo informa il master di aver finito, specificando le aziende che non ha trovato
        print("End download - node " + str(id))
        print(error_list)
        message = {
            'error_list' : error_list
        }
        client.publish("Node", json.dumps(message))
    
    elif (message['type'] == EVALUATION_TYPE):
        print(message['array'])
        correlation_list = evaluate_correlation(message['array'], message['T'])
        print(correlation_list)
        message = {
            'correlation_list' : correlation_list
        }
        client.publish("Node", json.dumps(message)) 
        global download_ended
        download_ended = True
    
    else: 
        print("Unknown message type")
        
while not(download_ended):
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_start()  
    client.loop_stop()

print("Process is ending")