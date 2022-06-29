# IMPORTS
from ete3 import Tree, NodeStyle, TreeStyle
import matplotlib.pyplot as plt
from tokenize import group
from ete3 import Tree, ClusterTree
import PyQt5
import pandas as pd
from pyparsing import matchPreviousExpr

#t = Tree("curto.treefile", format=1)
#t = Tree("62GH-afa.aln.treefile", format=1, quoted_node_names=True)
#t = Tree("GH116.aln.tree", format=1)
t = Tree("curto.tree", format=1, quoted_node_names=True)

# print(t.write(format=9))
#tree = Tree(t.write(format=9), quoted_node_names=True)
# print(tree)
# sed 's/::/../g' 62GH-afa.aln.treefile  > 62GH_renamed-afa.aln.treefile
#t = Tree("test.tree")
info = {}

for node in t.traverse("postorder"):
    if not node.is_leaf():
        try:
            node.support = float(node.name.split("/")[0]) # value of the support using fastbootstrap
            #node.support = float(node.name.split("/")[1])# value of the support using alrt
            print(node.support)
        except:
            print("no bootstrap")

#for node in t.traverse("postorder"):
#    node.name = node.name.split("_")[0]

#Getting midpoint outgroup for tree and rooting tree
midpoint_outgroup = t.get_midpoint_outgroup()
t.set_outgroup(midpoint_outgroup)

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
matrix2.to_csv(r'matrix2.txt', header=True, index=False, sep="\t")


print(f'Tree has {len(info.keys())} leaves/sequences')

# ver como colocar o que cada coluna significa
t = ClusterTree(t.write(), text_array="matrix2.txt")

# Setting Tree Style and Visualization
ts = TreeStyle()
ts.show_leaf_name = True
ts.show_scale = True
t.show("heatmap", tree_style=ts)