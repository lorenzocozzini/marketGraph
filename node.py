# subscriber
import paho.mqtt.client as mqtt
import sys
import pymongo
from datetime import datetime
import utils
from math import modf

def get_tickers(message, id_node, num_nodes):
    
    message = message[2:-2]
    list_msg = message.split('", "')
    len_list = len(list_msg)

    fraz, num_records = modf(len_list/num_nodes)
    #se non è l'ultimo 
    if (id_node + 1 < num_nodes):
        last_index = num_records*(id_node + 1)
    #se è l'ultimo
    else:
         last_index =len_list
    sub_list = list_msg[id_node*num_records:last_index] #qui leggo solo i primi 5, trovare modo intelligente per dividere (forse meglio fatto da master) TODO
    print("Downloading: ")
    print(sub_list)
    return sub_list

def update_data(stocklist):
    myclient = pymongo.MongoClient("mongodb://160.78.28.56:27017/") #160.78.28.56
    mydb = myclient["MarketDB"]
    #controllo quando ho fatto l'ultimo aggiornamento
    mycol = mydb[stocklist[0]]
    last_doc = mycol.find_one(
        sort=[( '_id', pymongo.DESCENDING )]
    )
    print(last_doc)
    if (last_doc != None):
        last_date = last_doc["Datetime"]
    else:
        last_date = datetime(2021, 6, 10) #TODO impostare default
    print(last_date)

    for ticker in stocklist:
        utils.download_finance(ticker=ticker, interval='1d', period1=last_date)

client = mqtt.Client()
client.connect('160.78.100.132', 9999)
#metti passAGGIO IP DA TERMINALE
id = int(sys.argv[1])    #py node.py 0
n_nodi = int(sys.argv[2]) 
b_msg = False

print("Node id: " + str(id))

def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Symbol")

def on_message(client, userdata, message):
    print(message.payload.decode())
    symbol_string = get_tickers(message.payload.decode(), id, n_nodi) 
    #aggiorna dati
    update_data(symbol_string)
    #informa master quando hai fatto
    print("Fine download")
    client.publish("Node", "DONE")

while True:
    client.on_connect = on_connect
    client.on_message = on_message #in loop perchè si disconnette dopo aver controllato messaggio
    client.loop_forever()
    