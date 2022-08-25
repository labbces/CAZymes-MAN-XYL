# IMPORTS
import argparse
from typing import Text
from ete3 import Tree, PhyloTree, TreeStyle, ClusterTree, NodeStyle, faces, AttrFace, ProfileFace, TextFace
import pandas as pd
from annotations import AnnotationOTUs
from annotations import AnnotationClusterCDHit
from annotations import AnnotationsECs
from annotations import AnnotationTreeGubbins

# Using argparse to handle variables
parser = argparse.ArgumentParser()
#parser.add_argument("--input",help="clstr output file from cd-hit",type=str,required=True)
#parser.add_argument("--output",help="prefix of output file",type=str,required=True)
#parser.add_argument("--family",help="facilitates identification in stdout",type=str,required=False)
parser.add_argument("--clusterfiles",help="information from clusters",type=str,required=False)
args = parser.parse_args()


# Loading TreeFile
#t = PhyloTree("phylo+teste.tree", format=1, sp_naming_function=None)
t = Tree("phylo+teste.tree", format=1)

# Annotation of OTUs using NCBI Taxonomy
AnnotationOTUs(t)

# Annotations of CD-HTI Hierarchical Clustering Data (treefile, shortened-seq-cluster,metadata)
#AnnotationClusterCDHit(t,"shortened-seq-cluster_GH62.csv","metadata_GH62.csv")

#Annotations of ECs Data(treefile, ECs_data)
#AnnotationsECs(t,"ECs/families.ECs.Proteins.txt")

# Annotations of TreeGubbins Data (treefile, treegubbins_data)
#AnnotationTreeGubbins(t,"rootedt62.csv")

# Setting Node Style
# nstyle = NodeStyle()
# nstyle["shape"] = "sphere"
# nstyle["size"] = 10
# nstyle["fgcolor"] = "darkred"
# nstyle["hz_line_type"] = 1
# nstyle["hz_line_color"] = "#cccccc"

# for node in t.traverse():
#     node.set_style(nstyle)

# for node in t.traverse():
#     try:
#         if node.EC:
#             nstyle = NodeStyle()
#             nstyle["fgcolor"] = "blue"
#             nstyle["size"] = 600
#             node.set_style(nstyle)
#     except:
#         continue
        


#Getting midpoint outgroup for tree and rooting tree
midpoint_outgroup = t.get_midpoint_outgroup()
t.set_outgroup(midpoint_outgroup)

# Handling with double support values
for node in t.traverse("postorder"):
    if not node.is_leaf():
        try:
            node.support = float(node.name.split("/")[0]) # value of the support using fastbootstrap
            #node.support = float(node.name.split("/")[1])# value of the support using alrt
        except:
            continue

# Setting Node Styles
nst1 = NodeStyle()
nst1["bgcolor"] = "Gold"
nst2 = NodeStyle()
nst2["bgcolor"] = "Moccasin"
nst3 = NodeStyle()
nst3["bgcolor"] = "DarkOrange"

for node in t.iter_leaves():
    node.name = node.name.split("_")[0]
    try:
        if node.superkingdom == "Bacteria":
            node.set_style(nst1)
        elif node.superkingdom == "Archaea":
            node.set_style(nst2)
        elif node.superkingdom == "Eukaryota":
            node.set_style(nst3)
    except:
        continue

# Adding faces to nodes with phylum information
for node in t.iter_leaves():
        name = node.phylum
        node.add_face(TextFace(name,fsize=20), position="branch-right",column=1)
      
# Tree description
#t.describe()

# Setting Tree Style and Visualization
ts = TreeStyle()
ts.show_leaf_name = True
ts.show_scale = True
ts.show_branch_length = True
ts.show_branch_support = True
ts.mode = "c"
#ts.arc_start = -180 # 0 degrees = 3 o'clock
#ts.arc_span = 180
#ts.root_opening_factor = 1
t.show(tree_style=ts)


# Writing the tree in newick format
#t.write(format=1, outfile="new_tree.nwk") 


# # Code for ClusterTree # #

# t = Tree("62GH-afa.aln.treefile", format=1, quoted_node_names=True)
# # print(t.write(format=9))
# tree = Tree(t.write(format=9), quoted_node_names=True)
# # print(tree)
# # sed 's/::/../g' 62GH-afa.aln.treefile  > 62GH_renamed-afa.aln.treefile
# #t = Tree("test.tree")
# info = {}

# for leaf in t.iter_leaves():
#     # print(leaf.name)
#     id = leaf.name
#     id_list = id.split('__')
# # Getting protein status info
#     status = id_list[1]
# # Getting protein group info
# #    tax = id_list[6]
# # print(tax)

#     info[id] = {}
#     if status != 'Predicted':
#         info[id]['Predicted'] = 0
#     else:
#         info[id]['Predicted'] = -2
#     if status != "structure":
#         info[id]['structure'] = 0
#     else:
#         info[id]['structure'] = -2
#     if status != "characterized":
#         info[id]['characterized'] = 0
#     else:
#         info[id]['characterized'] = -2
# '''
#     if tax != 'Bacteria':
#         info[id]['Bacteria'] = 0
#     else:
#         info[id]['Bacteria'] = -2
#     if tax != "Archaea":
#         info[id]['Archaea'] = 0
#     else:
#         info[id]['Archaea'] = -2
#     if tax != "Fungi":
#         info[id]['Fungi'] = 0
#     else:
#         info[id]['Fungi'] = -2
# '''

# matrix = pd.DataFrame.from_dict(info, orient='index')
# matrix2 = matrix.reset_index().rename({'index': '#Names'}, axis='columns')
# # print(matrix)
# matrix2.to_csv(r'matrix2.txt', header=True, index=False, sep="\t")


# print(f'Tree has {len(info.keys())} leaves/sequences')

# t = ClusterTree(tree.write(), text_array="matrix2.txt")

# # ver como colocar o que cada coluna significa
# t.show("heatmap")
# t.render(file_name='fileName.svg')