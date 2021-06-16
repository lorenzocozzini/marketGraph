#il master sa quanti nodi sono disponibile

#legge lista tickers e la divide?   ---> matrice chiave valore (0: [...], 1: [...] ecc) -> nodo 0 assegno lista0 ecc
#o la pubblica per intero e i nodi se la dividono?

import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import json

df = pd.read_csv('us_market.csv')
symbol_array = df["Symbol"].values
print(symbol_array)
print(len(symbol_array)) #7072

message = json.dumps(symbol_array.tolist())
print(message)

client = mqtt.Client()
client.connect('localhost', 9999)
client.publish("Symbol", message)  #NB: prima devono partire i nodi, altrimenti non leggono il messaggio pubblica

#dopo si iscrive a topic di tutti i nodi (o può essere anche lo stesso topic?)
done_msg = 0
n_nodes = 1 #TODO leggere da terminale

def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("Node")
    
def on_message(client, userdata, message):
    msg = message.payload.decode()
    print("MEssaggio: "+ msg)
    global done_msg
    if (msg == "DONE"):
        done_msg += 1 
        print(done_msg)

while (done_msg < n_nodes):
    client.on_connect = on_connect
    client.on_message = on_message 
    print("DONE: "+ str(done_msg))
    client.loop_start()    #https://stackoverflow.com/a/62950290
    #in mezzo fare cose che vogliamo
    client.loop_stop()
    
print("Fine scambio msg")

#TODO: elaborazione dati

#poi aspetta tot prima di ricominciare
    