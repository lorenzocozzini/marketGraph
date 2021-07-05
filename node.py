# subscriber
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
    #se non è l'ultimo 
    if (id_node + 1 < num_nodes):
        last_index = num_records*(id_node + 1)
    #se è l'ultimo
    else:
         last_index = len_list
    sub_list = list_msg[id_node*num_records:last_index] 
    #print("Downloading: ")
    #print(sub_list)
    return sub_list

def update_data(stocklist):
    error_list=[]
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB)) #160.78.28.56
    mydb = myclient["MarketDB"]
    bar = progressbar.ProgressBar(maxval=len(stocklist), \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    #controllo quando ho fatto l'ultimo aggiornamento
    i = 0
    bar.start()
    for ticker in stocklist:
        print(ticker)
        mycol = mydb[ticker]
        last_doc = mycol.find_one(
        sort=[( 'Datetime', pymongo.DESCENDING )] #prima _id
        )
        #print("LAST DOC:")
        #print(last_doc)
        if (last_doc != None):
            last_date = last_doc["Datetime"]
        else:
            last_date = datetime(2000, 1, 1) #Scarico dati dal 2000
        #print(last_date)
        if(download_finance(ticker=ticker, interval='1d', period1=last_date)==-1):
            error_list.append(ticker)
            
        bar.update(i) #sistemareeee
        sleep(0.2)
        i += 1
    
    bar.finish()      
    return error_list
       

client = mqtt.Client()
client.connect(IP_BROKER, 9999)
#metti passAGGIO IP DA TERMINALE
id = int(sys.argv[1])    #py node.py 0
n_nodi = int(sys.argv[2]) 
b_msg = False

print("Node id: " + str(id))

def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Symbol")

def on_message(client, userdata, message):
    #print(message.payload.decode())
    symbol_string = get_tickers(message.payload.decode(), id, n_nodi) 
    #aggiorna dati
    error_list= update_data(symbol_string)
    #informa master quando hai fatto
    print("Fine download - " + str(id))
    print(error_list)
    client.publish("Node", json.dumps(error_list)) 

while True:
    client.on_connect = on_connect
    client.on_message = on_message #in loop perchè si disconnette dopo aver controllato messaggio
    client.loop_forever()
    