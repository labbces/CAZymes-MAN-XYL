# IMPORTS
import argparse
from ete3 import Tree, PhyloTree, TreeStyle, ClusterTree, NodeStyle, faces, AttrFace, ProfileFace, TextFace
from ete3 import NCBITaxa; ncbi = NCBITaxa()
import matplotlib.pyplot as plt
from tokenize import group
from ete3 import Tree, ClusterTree
import PyQt5
import pandas as pd
from pyparsing import matchPreviousExpr

# Using argparse to handle variables
parser = argparse.ArgumentParser()
#parser.add_argument("--input",help="clstr output file from cd-hit",type=str,required=True)
#parser.add_argument("--output",help="prefix of output file",type=str,required=True)
#parser.add_argument("--family",help="facilitates identification in stdout",type=str,required=False)
parser.add_argument("--clusterfiles",help="information from clusters",type=str,required=False)
args = parser.parse_args()


# Annotation of OTUs using NCBI
t = PhyloTree("phylo+teste.tree", format=1, sp_naming_function=None)

for node in t.traverse("postorder"):
    try:
        if node.is_leaf():
            taxid = node.name.split("__")[7]
            lineage = ncbi.get_lineage(int(taxid))
            names = ncbi.get_taxid_translator(lineage)
            OTUs = [names[id] for id in lineage] # se usa o lineage ao inves do rank_names keys para se manter a ordem das OTUs
            ranks = ncbi.get_rank(lineage)
            life_structure = {}
            for i in range(len(OTUs)):
                life_structure[ranks[lineage[i]]] = OTUs[i]                    
            for feature,value in life_structure.items():
                if feature != "no rank":
                    node.add_feature(feature,value)
    except:
        print("Error:", node)

# Loading CD-HTI Hierarchical Clustering Data
cltdata = pd.read_csv("shortened-seq-cluster_GH62.csv", sep="\t", index_col=0)
metadata = pd.read_csv("metadata_GH62.csv", sep="\t")
not_studied_clt = cltdata.copy()
unique = metadata['cluster_id'].unique()
not_studied_clt = not_studied_clt[not_studied_clt['Cluster'].map(lambda x: x in unique)] 
nt_dict = not_studied_clt.to_dict() # convert cluster data into dictionary

# Annotation of CD-HTI Hierarchical Clustering Data
for node in t.traverse("postorder"):
    if node.is_leaf():
        try:
            id = node.name.split("_")[0].strip()
            if id in nt_dict['Cluster'].keys():      
                node.add_feature("cluster_cdhit", nt_dict['Cluster'][id])
            else:
                continue
        except:
             print("Parsing Error In:", node)

# Loading ECs Data
ECs = pd.read_csv("ECs/families.ECs.Proteins.txt", sep=";", index_col=1)
ECs = ECs[ECs['families'] == "GH62"]
ECs_dict = ECs.to_dict()
ECs_dict.keys()

# Annotation of ECs Data
for node in t.traverse("postorder"):
    if node.is_leaf():
        try:
            id = node.name.split("_")[0].strip()
            if id in ECs_dict['ECs'].keys():   
               node.add_feature("EC", ECs_dict['ECs'][id])  # add ECs to each leaf
               print(node.features)
        except:
             print("Parsing Error In:", node)


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
            

# Setting names equal IDs
for node in t.get_leaves():
    node.name = node.name.split("_")[0]

# Tree description
#t.describe()

# Setting Tree Style and Visualization
ts = TreeStyle()
ts.show_leaf_name = True
ts.show_scale = True
ts.mode = "c"
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