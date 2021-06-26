import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import pylab

G = nx.DiGraph()

#aggiungo archi con vari pesi
#noi avremo aziende e correlazioni
G.add_edges_from([('A', 'B'),('C','D'),('G','D')], weight=1) #aggiungi archi e dai un peso -> lo scrive 
G.add_edges_from([('D','A'),('D','E'),('B','D'),('D','E')], weight=2)
G.add_edges_from([('B','C'),('E','F')], weight=3)
G.add_edges_from([('C','F')], weight=4)


val_map = {'A': 1.0,
                   'D': 0.5714285714285714,
                              'H': 0.0}

values = [val_map.get(node, 0.45) for node in G.nodes()]
edge_labels=dict([((u,v,),d['weight'])
                 for u,v,d in G.edges(data=True)])
red_edges = [('C','D'),('D','A')]
edge_colors = ['black' if not edge in red_edges else 'red' for edge in G.edges()]

pos=nx.spring_layout(G)

#aggiunte silvia: nodi tutti rossi con nomi 
'''options = {
    'node_color': 'red',
    'node_size': 1000,
    'width': 3,
    'arrowstyle': '<->',
    'arrowsize': 12,
}
nx.draw_networkx(G, arrows=True, **options)'''

#codice originale
nx.draw_networkx_edge_labels(G,pos,edge_labels=edge_labels)
nx.draw(G,pos, node_color = values, node_size=1500,edge_color=edge_colors,edge_cmap=plt.cm.Reds)
pylab.show()