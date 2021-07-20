"""Graph2Vec illustrative example."""
from karateclub.graph_embedding import Graph2Vec
import pandas as pd
import networkx as nx
import numpy as np
from tensorflow.keras import models
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split
# Graph2Vec attributed example
import matplotlib.pyplot as plt
from math import sqrt
import json
import os

graphs = []
#vogliamo 250 features
dimensions = 250
'''for i in range(50):
    graph = nx.newman_watts_strogatz_graph(50, 5, 0.3)
    nx.set_node_attributes(graph, {j: str(j) for j in range(50)}, "feature")
    graphs.append(graph)
model = Graph2Vec(attributed=True)
model.fit(graphs)
model.get_embedding()'''

def dataset_reader(path):
    """
    Function to read the graph and features from a json file.
    :param path: The path to the graph json.
    :return graph: The graph object.
    :return features: Features hash table.
    :return name: Name of the graph.
    """
    name = path.strip(".json").split("/")[-1]
    data = json.load(open(path))
    graph = nx.from_edgelist(data["edges"])

    if "features" in data.keys():
        features = data["features"]
    else:
        features = nx.degree(graph)

    features = {int(k): v for k, v in features.items()}
    return graph, features, name

# Graph2Vec generic example

graphs = [nx.newman_watts_strogatz_graph(50, 5, 0.3) for _ in range(259)]
#da csv facciamo Json con tutti gli archi come in https://raw.githubusercontent.com/benedekrozemberczki/karateclub/master/dataset/graph_level/reddit10k/graphs.json
#leggi tutti i csv:
#tutti i csv in una cartella


""" list = os.listdir("C:\\Users\\Utente\\Desktop\\marketGraph")
number_files = len(list)
print(list) #lista di file
print("Numero di csv",number_files) """

model = Graph2Vec(dimensions=dimensions, workers = 4, epochs = 20, learning_rate = 0.025, min_count = 5)

model.fit(graphs)
#Stampo le X .. array in numpy
embedding = model.get_embedding()
#nome features
column_names = ["x_"+str(dim) for dim in range(dimensions)]
dfembedding = pd.DataFrame(embedding, columns=column_names)
dfembedding.to_csv('embedding.csv',index=False)

#BOOSTING

#RETE NEURALE

# TODO: Create the model
#[(Xf - Xi)/ Xi ] x 100 %
dataset = pd.read_csv('%5EGSPC.csv') #IL FILE NON E' IL DATO CHE VOGLIAMO
dataset = dataset['AdjClose']
#print(dataset)

#Fare percentuale per ogni mese
y = []
for i in range(1, len(dataset)):
    Xi = dataset[i-1]
    Xf = dataset[i]

    var_percentuale = ((Xf-Xi)/Xi)*100
    y.append(var_percentuale)

#print(y)
#y to numpy
y = np.array(y)

#rete
X = embedding
print(X.shape)
print(len(y))
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)

# Print the shapes of the new X objects
print("\nTraining set dimensions (X_train):")
print(X_train.shape)
print("\nTest set dimensions (X_test):")
print(X_test.shape)

# Print the shapes of the new y objects
print("\nTraining set dimensions (y_train):")
print(y_train.shape)
print("\nTest set dimensions (y_test):")
print(y_test.shape)


model = models.Sequential()
model.add(layers.Dense(12, activation='relu',input_shape=(X_train.shape[1],)))
model.add(layers.Dense(8, activation='relu'))
model.add(layers.Dense(1,activation='linear'))
model.compile(loss='binary_crossentropy', optimizer='adam',metrics = ['acc'])

history = model.fit(X_train, y_train, epochs=800, batch_size=200,validation_split=0.2, verbose=1)
test_loss,test_acc_score = model.evaluate(X_test,y_test)
print(sqrt(test_acc_score))


#PLOT
plt.figure()
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.plot(history.epoch,
np.array(history.history['loss']),label='Train loss')
plt.plot(history.epoch,
np.array(history.history['val_loss']),label = 'Val loss')
plt.legend()
plt.show()


