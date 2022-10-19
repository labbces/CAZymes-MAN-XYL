import argparse
import pandas as pd
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help=" representatives sequences from cd-hit", type=str, required=True)
parser.add_argument("-o", "--output", help="output file", type=str, required=True)
parser.add_argument("--metadata",help="data from not studied clusters",type=str,required=True)
parser.add_argument("--clusterseqs",help="seqs with they representative cluster, output from hc-parsing",type=str,required=True)
parser.add_argument("--substrate",help="clusterinfo.csv, information about substrate in cluster",type=str,required=True)
parser.add_argument("-cn", "--clusternumber",help="number of clusters that constains xylan",type=str,required=True)
args = parser.parse_args()

# Getting representative sequences from cd-hit
try:
    original_seq_handler = open(args.input, 'r')
except:
    print("File", args.input, "cannot be open")
    exit()

# Creating newfile with representative sequences ID
try:
    Ids = open(args.output, 'w')
except:
    print("File", args.output, "cannot be open")
    
# File with number of clusters that contains xylan
try:
    xylancluster = open(args.clusternumber, 'w')
except:
    print("File", args.clusternumber, "cannot be open")


# Selecting not representative sequences ID
clusters = pd.read_csv(args.clusterseqs, sep="\t", index_col=0)    
metadata = pd.read_csv(args.metadata, sep="\t")
not_studied_clusters = clusters.copy()
unique = metadata['cluster_id'].unique()
not_studied_clusters = not_studied_clusters[not_studied_clusters['Cluster'].map(lambda x: x in unique)] 
nt_dict = not_studied_clusters.to_dict() # convert cluster data into dictionary


# Selecting by substrate
cs = pd.read_csv(args.substrate, sep=",", index_col=0)
substrate = cs[["Substrate"]].copy()
substrate.index = substrate.index.map(lambda x: x - 1)
substrate = substrate.to_dict()
    
# Writing representative sequences ID
for line in original_seq_handler:
    if line.startswith('>'):
        line = line.split("+")[0].split(">")[1]  # Removing the rest of the line with plus
        #line = line.split(" ")[0].split(">")[1]  # Removing the rest of the line with three points
        if line in nt_dict['Cluster'].keys():
            try:
                if substrate["Substrate"][nt_dict['Cluster'][line]] == "['Xylan']":
                    Ids.write(line+"\n")
                    xylancluster.write(str(nt_dict['Cluster'][line])+"\n")
                elif substrate["Substrate"][nt_dict['Cluster'][line]] == "['Mannan', 'Xylan']":
                    Ids.write(line+"\n")
                    xylancluster.write(str(nt_dict['Cluster'][line])+"\n")
                elif substrate["Substrate"][nt_dict['Cluster'][line]] == "['Xylan', 'Mannan']":
                    Ids.write(line+"\n")
                    xylancluster.write(str(nt_dict['Cluster'][line])+"\n")
            except:
                continue
Ids.close()