#calcolo correlazione
import multiprocessing
import pandas as pd
import json

def worker():
    df = pd.read_csv('us_market.csv')
    symbol_array = df["Symbol"].values
    message = json.dumps(symbol_array.tolist())
    id = int(multiprocessing.current_process().name)
    sub_list = message[0:16]
    print (multiprocessing.current_process().name," Worker")
    return

if __name__ == '__main__':
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(name =str(i),target=worker)
        jobs.append(p)
        p.start()
