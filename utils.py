import time
import datetime
from datetime import datetime, timedelta
import pandas as pd
import pymongo
import json
import requests
from math import sqrt
from scipy.stats import norm
from math import sqrt
import scipy.stats as st
from datetime import datetime 
import matplotlib.pyplot as plt
import numpy as np
from json.decoder import JSONDecodeError

IP_BROKER = '160.78.100.132'
IP_MONGO_DB = '160.78.28.56'
timeStart = 0
DOWNLOAD_TYPE = 0
EVALUATION_TYPE = 1

def delete_duplicates(arraylist):
    new_list = []
    for x in arraylist:
        if x not in new_list:
            new_list.append(x)
    return new_list

def download_finance(ticker, interval, period1, period2 = datetime.now()):
    period1 = int(time.mktime(period1.timetuple()))
    period2 = int(time.mktime(period2.timetuple()))
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    query_string = f'https://query1.finance.yahoo.com/v7/finance/chart/{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
    print(query_string)
    response = requests.get(query_string, headers=headers)
    
    try:
        #print(response)
        data = json.loads(response.content.decode())

        # Se è presente un errore nella richiesta, si salva il ticker nella lista da mandare indietro al master
        if (data['chart']['error'] != None):
            print(ticker + ' ' + data['chart']['error']['code']) 
            #if (data['chart']['error']['code']== "Bad Request"):
            return -1 

        # Se la richiesta non presenta errori ma non sono presenti dati, significa che tutti i dati sono già stati scaricati in precedenza
        if (data['chart']['result'][0]['indicators']['quote'][0] == {}):
            print(ticker + ' already updated')
            return 0 

        # Parsing della risposta
        open = data['chart']['result'][0]['indicators']['quote'][0]['open']
        high = data['chart']['result'][0]['indicators']['quote'][0]['high']
        low = data['chart']['result'][0]['indicators']['quote'][0]['low']
        close = data['chart']['result'][0]['indicators']['quote'][0]['close']
        adj_close = data['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
        volume = data['chart']['result'][0]['indicators']['quote'][0]['volume']

        timestamp = []
        for i in data['chart']['result'][0]['timestamp']:
            data = datetime.fromtimestamp(i)
            data = data.strftime("%Y-%m-%dT%H:%M:%S")
            datetimeData = pd.to_datetime(i, unit="s")
            timestamp.append(datetimeData)

        myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))
        mydb = myclient["MarketDB"]
        mycol = mydb[ticker]

        # Si aggiungono i dati al DB, giorno per giorno, verificando che non siano già presenti dati per la stessa giornatar
        for i in range(len(timestamp)):
            object = {"Datetime": timestamp[i],
                    "Open": open[i],
                    "High": high[i],
                    "Low": low[i],
                    "Close": close[i],
                    "AdjClose": adj_close[i],
                    "Volume": volume[i]
                    }
            
            start= datetime(timestamp[i].year, timestamp[i].month, timestamp[i].day)
            end= datetime(timestamp[i].year, timestamp[i].month, timestamp[i].day, 23, 59, 59)
            query = {'Datetime': {'$gte': start , '$lt': end}}
            find_date = mycol.find_one(query)
            
            # Se non sono presenti dati per quel determinato giorno, è possibile aggiungere quelli scaricati 
            if (find_date == None):
                mycol.insert_one(object) 
            else:
                print(ticker + " already in DB ({})".format(start.date()))
        return 0
    except JSONDecodeError as e:
        print('Decoding JSON has failed')

def get_adj_close(ticker, T):
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))
    mydb = myclient["MarketDB"]
    mycol = mydb[ticker]
    cursor = mycol.find(
        sort = [( 'Datetime', pymongo.DESCENDING )], 
        limit= T
    )
    last_doc = list(cursor)
    
    datetime = []
    adj_close = []
    for j in last_doc:
        datetime.append(j["Datetime"].date().isoformat())
        adj_close.append(j["AdjClose"])
    return datetime, adj_close

def get_correlation(adj_close_1, adj_close_2, T):
  
    # Calcolo componenti numeratore 
    product = [x*y for x,y in zip(adj_close_1,adj_close_2)]
    arg1 = sum(product)/T
    
    r1_brack = sum(adj_close_1)/T
    r2_brack = sum(adj_close_2)/T
    arg2 = r1_brack * r2_brack
    
    # Calcolo componenti denominatore  
    r1_quad = [x*y for x,y in zip(adj_close_1,adj_close_1)]
    r2_quad = [x*y for x,y in zip(adj_close_2,adj_close_2)]
    
    r1_quad_sottr = [x - (r1_brack*r1_brack) for x in r1_quad]
    r2_quad_sottr = [x - (r2_brack*r2_brack) for x in r2_quad]
    
    arg3 = sum(r1_quad_sottr)/T
    arg4 = sum(r2_quad_sottr)/T
    
    if (sqrt(arg3*arg4) != 0):
        correlation = (arg1 -arg2)/sqrt(arg3*arg4)
    # Se il denominatore è nullo, la correlazione è indefinita
    else:
        correlation = 0 
    return correlation

def get_threshold(correlation_list):
    
    # Estrazione delle correlazioni dalla lista
    correlation_value = []
    #for i in range(len(correlation_list)):
    for tupla in correlation_list:#[i]:
        correlation_value.append(tupla[2])

    # Calcolo dell'istogramma e della Gaussiana
    mu, std = norm.fit(correlation_value) 
    plt.hist(correlation_value, density=True, bins=20, label="Data")
    mn, mx = plt.xlim()
    plt.xlim(mn, mx)
    kde_xs = np.linspace(mn, mx, 30)
    kde = st.gaussian_kde(correlation_value)
    plt.plot(kde_xs, kde.pdf(kde_xs), label="PDF")
    plt.legend(loc="upper left")
    plt.ylabel('Probability')
    plt.xlabel('Data')
    title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
    plt.title(title)
    
    plt.savefig('histogram_gaussian.png', bbox_inches='tight')
    #plt.show() 

    r1 = np.mean(correlation_value)
    print("Mean: ", r1)
    r2 = np.std(correlation_value)
    print("Std: ", r2)
    r3 = np.var(correlation_value)
    print("Variance: ", r3)

    std = r2
    return std

def get_edges(theta, corr_list):
    edges_list=[]
    for tupla in corr_list:
        if (tupla[2]>=theta):
            edges_list.append(tupla)
    return edges_list

def start_timer():
    global timeStart
    timeStart  = time.time() * 1000 #secondi
    interval = 0
    return interval
    
def increase_timer(interval):
    global timeStart
    timeStop = time.time() * 1000
    interval += (timeStop - timeStart) / 1000 #secondi
    timeStart = timeStop
    return interval

def get_symbol_array():
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))
    mydb = myclient["MarketDB"]
    mycol = mydb["Markets"]
    results = mycol.find({}, {'Symbol': 1, '_id':0})
    symbol_array = []
    for x in results:
        if x["Symbol"] not in symbol_array:
            symbol_array.append(x["Symbol"])
    
    if (len(symbol_array) == 0):
        df = pd.read_csv('us_market.csv')
        symbol_array = df["Symbol"].values
        symbol_array = delete_duplicates(symbol_array)
    return(symbol_array)