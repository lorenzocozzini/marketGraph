# subscriber
import paho.mqtt.client as mqtt
import sys
import pymongo
from datetime import datetime
import utils

def get_tickers(message, idNode):
    print(type(message)) 
    message = message[2:-2]
    list_msg = message.split('", "')
    print(list_msg) 
    
    sub_list = list_msg[0:5] #qui leggo solo i primi 5, trovare modo intelligente per dividere (forse meglio fatto da master) TODO
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
id = int(sys.argv[1])    #py node.py 0

b_msg = False

print("Node id: " + str(id))
    
def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Symbol")

def on_message(client, userdata, message):
    print(message.payload.decode())
    symbol_string = get_tickers(message.payload.decode(), id) 
    #aggiorna dati
    update_data(symbol_string)
    #informa master quando hai fatto
    print("Fine download")
    client.publish("Node", "DONE")

while True:
    client.on_connect = on_connect
    client.on_message = on_message #in loop perchè si disconnette dopo aver controllato messaggio
    client.loop_forever()
    