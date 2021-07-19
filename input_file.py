import networkx as nx
import pandas as pd
import glob
import os

graphs = []
#G = nx.from_pandas_edgelist(df, edge_attr='weight', create_using=nx.Graph())
#G = nx.read_edgelist("correlation.csv", delimiter=",", data=[("weight", float)])  #c'è da cancellare la prima riga
# use glob to get all the csv files 
# in the folder
path = os.getcwd()
csv_files = glob.glob(os.path.join(path, "dataset/*.csv"))
  
  
# loop over the list of csv files
for f in csv_files:
    print(f)
    G = nx.read_edgelist(f, delimiter=",", data=[("weight", float)])  #c'è da cancellare la prima riga
    

G.edges(data=True)
print(G)
graphs.append(G)


edge_labels = dict( ((u, v), d["weight"]) for u, v, d in G.edges(data=True) )
pos = nx.random_layout(G)
nx.draw(G, pos)
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
import matplotlib.pyplot as plt
plt.show()


