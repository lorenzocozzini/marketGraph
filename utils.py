import time
import datetime
from datetime import datetime
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

IP_BROKER = 'localhost' #'160.78.100.132'
IP_MONGO_DB = 'localhost' #'160.78.28.56'
timeStart = 0

def delete_duplicates(arraylist):
    new_list = []
    for x in arraylist:
        if x not in new_list:
            new_list.append(x)
    return new_list

def download_finance(ticker, interval, period1, period2 = datetime.now()):
    
    period1 = int(time.mktime(period1.timetuple()))
    #period2 = period2.replace(hour=14, minute=30, second=0, microsecond=0)
    period2 = int(time.mktime(period2.timetuple()))

    query_string = f'https://query1.finance.yahoo.com/v7/finance/chart/{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
    
    #print(query_string)
    response = requests.get(query_string)
    data = json.loads(response.content.decode())
    #print(data)

    if (data['chart']['error'] != None):
        print(ticker + ' ' + data['chart']['error']['code']) 
        return -1 #perchè errore

    #print(data['chart']['result'][0]['indicators']['quote'][0])
    if (data['chart']['result'][0]['indicators']['quote'][0] == {}):
        print(ticker + ' already updated')
        return 0 #va tutto ok

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


    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))  #160.78.28.56
    mydb = myclient["MarketDB"]
    mycol = mydb[ticker]

    for i in range(len(timestamp)):
        object = {"Datetime": timestamp[i],
                "Open": open[i],
                "High": high[i],
                "Low": low[i],
                "Close": close[i],
                "AdjClose": adj_close[i],
                "Volume": volume[i]
                }
        
        #start = timestamp[i].date()
        start= datetime(timestamp[i].year, timestamp[i].month, timestamp[i].day)
        end= datetime(timestamp[i].year, timestamp[i].month, timestamp[i].day, 23, 59, 59)
        query = {'Datetime': {'$gte': start , '$lt': end}}
        find_date = mycol.find_one(query)
        
        #find_date = mycol.find_one({"Datetime":timestamp[i]})
        if (find_date == None):
            mycol.insert_one(object)  #upsert = true
        else:
            print(ticker + " already in DB")
    return 0

def get_adj_close(ticker, T):
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB)) #160.78.28.56
    mydb = myclient["MarketDB"]
    mycol = mydb[ticker]
    cursor = mycol.find(
    sort = [( 'Datetime', pymongo.DESCENDING )], 
    limit= T #numero di giorni che vogliamo
    )
    last_doc = list(cursor)
    #print(last_doc)
    adj_close = []
    for j in last_doc:
        adj_close.append((j["Datetime"], j["AdjClose"]))
    return adj_close

def same_date(adj_close_1, adj_close_2):
    for i in range(len(adj_close_1)):
        if (adj_close_1[i][0] != adj_close_2[i][0]):
            return False
            
    return True

def get_correlation(tupla_1, tupla_2, T):
    adj_close_1 = []
    adj_close_2 = []
    for i in range(len(tupla_1)):
        adj_close_1.append(tupla_1[i][1])
        adj_close_2.append(tupla_2[i][1])
  
    #arg1
    product = [x*y for x,y in zip(adj_close_1,adj_close_2)]
    arg1 = sum(product)/T
    
    #arg2
    r1_brack = sum(adj_close_1)/T
    r2_brack = sum(adj_close_2)/T
    arg2 = r1_brack * r2_brack
    
    r1_quad = [x*y for x,y in zip(adj_close_1,adj_close_1)]
    r2_quad = [x*y for x,y in zip(adj_close_2,adj_close_2)]
    
    r1_quad_sottr = [x - (r1_brack*r1_brack) for x in r1_quad]
    r2_quad_sottr = [x - (r2_brack*r2_brack) for x in r2_quad]
    
    arg3 = sum(r1_quad_sottr)/T
    arg4 = sum(r2_quad_sottr)/T
    
    if (sqrt(arg3*arg4) != 0):
        corr_mantegna = (arg1 -arg2)/sqrt(arg3*arg4)
    else:
        corr_mantegna = 0 #indefinita
    return corr_mantegna

def get_hist(corr_list):
    
    #correlation_count = [0] * 10

    #calcolo istogramma per ogni range di valori per le correlazioni
    
    correlation_value = []
  
    for i in range(len(corr_list)):
        for tupla in corr_list[i]:
            correlation_value.append(tupla[2])
            """ if(tupla[2] > 0.0 and tupla[2] <= 0.1):
                correlation_count[0]+=1
            elif(tupla[2] > 0.1 and tupla[2] <= 0.2):
                correlation_count[1]+=1
            elif(tupla[2] > 0.2 and tupla[2] <= 0.3):
                correlation_count[2]+=1
            elif(tupla[2] > 0.3 and tupla[2] <= 0.4):
                correlation_count[3]+=1
            elif(tupla[2] > 0.4 and tupla[2] <= 0.5):
                correlation_count[4]+=1
            elif(tupla[2] > 0.5 and tupla[2] <= 0.6):
                correlation_count[5]+=1
            elif(tupla[2] > 0.6 and tupla[2] <= 0.7):
                correlation_count[6]+=1
            elif(tupla[2] > 0.7 and tupla[2] <= 0.8):
                correlation_count[7]+=1
            elif(tupla[2] > 0.8 and tupla[2] <= 0.9):
                correlation_count[8]+=1
            elif(tupla[2] > 0.9 and tupla[2] <= 1.0):
                correlation_count[9]+=1 """
                
    #print(correlation_value)
    
    """ #disegno istogramma
    indices = np.arange(len(correlation_count))
    word = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    plt.bar(indices, correlation_count, color='r')
    plt.xticks(indices, word, rotation='vertical')
    plt.tight_layout()
    plt.show()
    """
    #calcolo Gaussiana
    mu, std = norm.fit(correlation_value) 
    plt.hist(correlation_value, density=True, bins=20, label="Data") #diminuire bins
    mn, mx = plt.xlim()
    plt.xlim(mn, mx)
    kde_xs = np.linspace(mn, mx, 20)
    kde = st.gaussian_kde(correlation_value)
    plt.plot(kde_xs, kde.pdf(kde_xs), label="PDF")
    plt.legend(loc="upper left")
    plt.ylabel('Probability')
    plt.xlabel('Data')
    title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
    plt.title(title)
    
    fig = plt.figure()
    fig.savefig("histogram_gaussian.png",  dpi=fig.dpi)
    plt.savefig('foo.png', bbox_inches='tight')
    plt.show()

    r1 = np.mean(correlation_value)
    print("\nMean: ", r1)
    r2 = np.std(correlation_value)
    print("\nstd: ", r2)
    r3 = np.var(correlation_value)
    print("\nvariance: ", r3)

    return std

def get_edges(theta, corr_list):
    edges_list=[]
    for i in range(len(corr_list)):
        for tupla in corr_list[i]:
            if (tupla[2]>=theta):
                edges_list.append(tupla)
    return edges_list


def start_timer():
    global timeStart
    timeStart  = time.time() * 1000000000
    interval = 0
    return interval
    
def increase_timer(interval):
    global timeStart
    timeStop = time.time() * 1000000000
    interval += (timeStop - timeStart) / 1000000
    timeStart = timeStop
    return interval

def get_symbol_array():
    myclient = pymongo.MongoClient("mongodb://{}:27017/".format(IP_MONGO_DB))  #160.78.28.56
    mydb = myclient["MarketDB"]
    mycol = mydb["Markets"]
    results = mycol.find({}, {'Symbol':1, '_id':0})
    symbol_array = []
    for x in results:
        if x["Symbol"] not in symbol_array:
            symbol_array.append(x["Symbol"])
    
    if (len(symbol_array) == 0):
        df = pd.read_csv('us_market.csv')
        symbol_array = df["Symbol"].values
        symbol_array = delete_duplicates(symbol_array)
    return(symbol_array)