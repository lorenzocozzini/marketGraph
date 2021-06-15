#il master sa quanti nodi sono disponibile
    #TODO: se ci sono 6 nodi, master si iscrive a 6 topic nella forma es "node0"

#legge lista tickers e la divide?   ---> matrice chiave valore (0: [...], 1: [...] ecc) -> nodo 0 assegno lista0 ecc
#o la pubblica per intero e i nodi se la dividono?

import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np

df = pd.read_csv('us_market.csv')
symbol_array = df["Symbol"].values
print(symbol_array)
print(len(symbol_array)) #7072

symbol_string = np.array2string(symbol_array, separator=',') #TODO cambiare
print(type(symbol_string))

client = mqtt.Client()
client.connect('localhost', 9999)
client.publish("Symbol", symbol_string)  #NB: prima devono partire i nodi, altrimenti non leggono il messaggio pubblica

#dopo si iscrive a topic di tutti i nodi (o può essere anche lo stesso topic?)
done = 0
n_nodes = 1 #leggere da terminale?
"""
def on_message(client, userdata, message):
    msg = message.payload.decode()
    print(msg)
    if (msg == "DONE"):
        done += 1 #perchè non lo prendeeee TODO 

while (done < n_nodes):
    client.subscribe("Node")
    client.on_message = on_message 
    
"""
#finito elabora i dati
#poi aspetta tot prima di ricominciare
    