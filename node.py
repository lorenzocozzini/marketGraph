# subscriber
import paho.mqtt.client as mqtt
import sys

import yfinance as yf
import pymongo
from datetime import datetime
import numpy as np

def get_tickers(message, idNode):
    message = message[2:-2]
    list_msg = message.split("','")
    print(list_msg) #['AACG', 'AACQ', "AACQU',...,'ZTS", 'ZUO', 'ZYME']
    
    print(list_msg[0:5]) #['AACG', 'AACQ', "AACQU',...,'ZTS", 'ZUO', 'ZYME'] -> il to string che fa il master nel publish non va bene
    
    #sub_list = list_msg[0:5] #qui leggo solo i primi 5, trovare modo intelligente per dividere (forse meglio fatto da master)
                            #nodi sanno quanti sono? se no meglio da master da bo
    sub_list = []
    print(sub_list)
    return sub_list



def update_data(stocklist):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
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
        last_date = datetime(2021, 6, 10) #impostare default
    print(last_date)

    data = yf.download(
        tickers = stocklist,
        start=last_date,  #end def is now
        interval = "5m",
        group_by = 'ticker',
        prepost = True,
        threads = True,
        proxy = None
    )

    #print(data)
    for ticker in stocklist:
        mycol = mydb[ticker]
        data_tick = data[ticker]
        data_tick.reset_index(inplace=True) 
        data_dict = data_tick.to_dict("records") 
        i = 0
        for i in range(len(data_dict)):
            mycol.insert_one(data_dict[i]) 
            

client = mqtt.Client()
client.connect('localhost', 9999)
id = int(sys.argv[1])    #py node.py 0

b_msg = False

print("Node id: " + str(id))
    
def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Symbol")

def on_message(client, userdata, message):
    symbol_string = get_tickers(message.payload.decode(), id)
    print(symbol_string)
    #aggiorna dati
    update_data(symbol_string)
    #informa master quando hai fatto
    client.publish("Node", "DONE")

while True:
    client.on_connect = on_connect
    client.on_message = on_message #in loop perchè si disconnette dopo aver controllato messaggio
    client.loop_forever()
    