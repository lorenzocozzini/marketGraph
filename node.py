import paho.mqtt.client as mqtt
import sys
import pymongo
from datetime import datetime
from utils import IP_BROKER, IP_MONGO_DB, download_finance
import json
from math import modf
import progressbar
from time import sleep

def get_tickers(message, id_node, num_nodes):
    message = message[2:-2]
    list_msg = message.split('", "')
    len_list = len(list_msg)

    fraz, num_records = modf(len_list/num_nodes)
    num_records = int(num_records)
    # Assegnazione delle aziende ai singoli nodi per il download dei dati aggiornati
    if (id_node + 1 < num_nodes):
        last_index = num_records*(id_node + 1)
    # L'ultimo nodo prende tutti i ticker rimasti
    else:
         last_index = len_list
    sub_list = list_msg[id_node*num_records:last_index] 
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

client = mqtt.Client()
client.connect(IP_BROKER, 9999)

id = int(sys.argv[1])
n_nodi = int(sys.argv[2])

print("Node id: " + str(id))

def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Symbol")

def on_message(client, userdata, message):
    # Alla ricezione di un messaggio, il nodo legge la lista e estrae le aziende a lui assegnate 
    symbol_list = get_tickers(message.payload.decode(), id, n_nodi) 
    # Poi scarica i dati, tenendo traccia delle aziende per cui ha avuto problemi di download
    error_list = update_data(symbol_list)
    # Al termine, il nodo informa il master di aver finito, specificando le aziende che non ha trovato
    print("End download - node " + str(id))
    print(error_list)
    client.publish("Node", json.dumps(error_list)) 

while True:
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()
    