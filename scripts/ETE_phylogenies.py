# IMPORTS
import argparse
from typing import Text
from ete3 import Tree, PhyloTree, TreeStyle, ClusterTree, NodeStyle, faces, AttrFace, ProfileFace, TextFace
import pandas as pd
from annotations import AnnotationOTUs
from annotations import AnnotationClusterCDHit

# Using argparse to handle variables
parser = argparse.ArgumentParser()
#parser.add_argument("-t", "--tree", help="Input tree file", type=str, required=True)
#parser.add_argument("--metadata",help="data from not studied clusters",type=str,required=False)
#parser.add_argument("--clusterseqs",help="seqs with they representative cluster, output from hc-parsing",type=str,required=False)
#parser.add_argument("--substrate",help="clusterinfo.csv, information about substrate in cluster",type=str,required=False)
args = parser.parse_args()

# Loading TreeFile
t = Tree("GH98.tree", format=1)

# Annotation of OTUs using NCBI Taxonomy
AnnotationOTUs(t)

# Annotations of CD-HTI Hierarchical Clustering Data (treefile, shortened-seq-cluster,metadata)
AnnotationClusterCDHit(t,"shortened-seq-cluster_GH98.csv","metadata_GH98.csv")


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
    try:
        name = node.phylum
        node.add_face(TextFace(name,fsize=20), position="branch-right",column=1)
    except:
        continue

# Adding substrate information to nodes
clusters = pd.read_csv("GH98_clusterInfo.csv", sep=",", index_col=0)
substrate = clusters[["Substrate"]].copy()
substrate = substrate.to_dict()

for node in t.traverse("postorder"): 
    if node.is_leaf():
        try:
            if node.cdhit_cluster in substrate["Substrate"].keys(): 
                if len(substrate["Substrate"][node.cdhit_cluster]) > 10:
                    node.add_feature("substrate", "both")
                    print(node.substrate)
                elif substrate["Substrate"][node.cdhit_cluster] == "['Mannan']":
                    node.add_feature("substrate", "Mannan")
                    print(node.substrate)
                elif substrate["Substrate"][node.cdhit_cluster] == "['Xylan']":
                    node.add_feature("substrate", "Xylan")
                    print(node.substrate)
                #node.add_feature("substrate", substrate["Substrate"][node.cdhit_cluster])
                else:
                    continue
                
        except:
            continue


# Tree description
#t.describe()

# Setting Tree Style and Visualization
# ts = TreeStyle()
# ts.show_leaf_name = False
# ts.show_scale = True
# ts.show_branch_length = True
# ts.show_branch_support = True
# ts.mode = "c"
# #ts.arc_start = -180 # 0 degrees = 3 o'clock
#ts.arc_span = 180
#ts.root_opening_factor = 1
#t.show(tree_style=ts)


# Writing the tree in newick format
#t.write(format=1, outfile="new_tree.nwk") 

