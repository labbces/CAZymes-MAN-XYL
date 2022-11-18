# Imports
import argparse
import pandas as pd

# Handling arguments from command line
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
    
# File clusters that contains xylan
try:
    xylancluster = open(args.clusternumber, 'w')
except:
    print("File", args.clusternumber, "cannot be open")

# Selecting not studied representative sequences ID
clusterseqs = pd.read_csv(args.clusterseqs, sep="\t", index_col=0)    
metadata = pd.read_csv(args.metadata, sep="\t")
not_studied_clusters = clusterseqs.copy()
ids = metadata['cluster_id'].unique()
not_studied_clusters = not_studied_clusters[not_studied_clusters['Cluster'].map(lambda x: x in ids)] 
not_studied_sequences = not_studied_clusters['Cluster'].map(lambda x: x + 1) # adding 1 to cluster id to fix unmatching index
nt_studied_seqs_dict = not_studied_sequences.to_dict() # convert cluster data into dictionary

# Selecting substrate information
cs = pd.read_csv(args.substrate, sep=",", index_col=0)
substrate = cs[["Substrate"]].copy()
substrate = substrate.to_dict()
print(substrate)

# Getting representative sequences ID not studied and with xylan
for line in original_seq_handler:
            if line.startswith('>'):
                ncbi_id = line.split("+")[0].split(">")[1]  # Removing the rest of the line with plus
                #line = line.split(" ")[0].split(">")[1]  # Removing the rest of the line with three points
                if ncbi_id in nt_studied_seqs_dict.keys():
                    try:
                        if substrate["Substrate"][nt_studied_seqs_dict[ncbi_id]] == "['Xylan']":
                            Ids.write(ncbi_id+"\n")
                            xylancluster.write(str(nt_studied_seqs_dict[ncbi_id])+"\n")
                        elif substrate["Substrate"][nt_studied_seqs_dict[ncbi_id]] == "['Mannan', 'Xylan']":
                            Ids.write(ncbi_id+"\n")
                            xylancluster.write(str(nt_studied_seqs_dict[ncbi_id])+"\n")
                        elif substrate["Substrate"][nt_studied_seqs_dict[ncbi_id]] == "['Xylan', 'Mannan']":
                            Ids.write(ncbi_id+"\n")
                            xylancluster.write(str(nt_studied_seqs_dict[ncbi_id])+"\n")
                    except KeyError:
                        pass
Ids.close()
xylancluster.close()
