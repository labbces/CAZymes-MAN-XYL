# IMPORTS
#from ete3 import Tree, NodeStyle, TreeStyle
import matplotlib.pyplot as plt
from tokenize import group
from ete3 import Tree, ClusterTree
import PyQt5
import pandas as pd
from pyparsing import matchPreviousExpr


t = Tree("62GH-afa.aln.treefile", format=1, quoted_node_names=True)
# print(t.write(format=9))
tree = Tree(t.write(format=9), quoted_node_names=True)
# print(tree)
# sed 's/::/../g' 62GH-afa.aln.treefile  > 62GH_renamed-afa.aln.treefile
#t = Tree("test.tree")
info = {}

for leaf in t.iter_leaves():
    # print(leaf.name)
    id = leaf.name
    id_list = id.split('__')
# Getting protein status info
    status = id_list[1]
# Getting protein group info
#    tax = id_list[6]
# print(tax)

    info[id] = {}
    if status != 'Predicted':
        info[id]['Predicted'] = 0
    else:
        info[id]['Predicted'] = -2
    if status != "structure":
        info[id]['structure'] = 0
    else:
        info[id]['structure'] = -2
    if status != "characterized":
        info[id]['characterized'] = 0
    else:
        info[id]['characterized'] = -2
'''
    if tax != 'Bacteria':
        info[id]['Bacteria'] = 0
    else:
        info[id]['Bacteria'] = -2
    if tax != "Archaea":
        info[id]['Archaea'] = 0
    else:
        info[id]['Archaea'] = -2
    if tax != "Fungi":
        info[id]['Fungi'] = 0
    else:
        info[id]['Fungi'] = -2
'''

matrix = pd.DataFrame.from_dict(info, orient='index')
matrix2 = matrix.reset_index().rename({'index': '#Names'}, axis='columns')
# print(matrix)
matrix2.to_csv(r'matrix2.txt', header=True, index=False, sep="\t")


print(f'Tree has {len(info.keys())} leaves/sequences')

t = ClusterTree(tree.write(), text_array="matrix2.txt")

# ver como colocar o que cada coluna significa
t.show("heatmap")
t.render(file_name='fileName.svg')
