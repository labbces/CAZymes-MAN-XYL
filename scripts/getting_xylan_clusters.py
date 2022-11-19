import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-cn", "--clusternumber", help="full path from xylan cluster number file", type=str, required=True)
parser.add_argument("-o", "--output", help="output file", type=str, required=True)
args = parser.parse_args()

# Getting xylan clusters
with open(args.clusternumber, 'r') as xylan_clusters: 
    xylan_clusters_list =  xylan_clusters.read().splitlines()
    
# Getting family/path 
family = args.clusternumber.split('/')[6].split('_')[0]

# checking if the path exists
if not os.path.exists(f'{args.output}/{family}'):
    os.makedirs(f'{args.output}/{family}')

missing = []

# Creating a file with xylan clusters sequences
count = 0
for cluster in xylan_clusters_list:
    try:
        with open(f"/home/dan/CAZymes-MAN-XYL/grupos/{family}/{family}_cluster{cluster}.fasta", 'r') as cluster_file:
            with open(f'{args.output}/{family}/{family}_cluster{cluster}ids.txt', 'w') as Ids:
                for line in cluster_file:
                    if line.startswith('>'):
                        count += 1
                        #line = line.split("+")[0].split(">")[1]  # Getting ID, breaking plus sign
                        line = line.split(" ")[0].split(">")[1]  # Getting ID, breaking space
                        Ids.write(line+"\n")
    except:
        print(f"{family}_cluster{cluster}.fasta not found")
        missing.append(f"{family}_cluster{cluster}.fasta")
        with open(f'{args.output}/{family}/missing_clusters.txt', 'w') as missing_clusters:
            for cluster in missing:
                missing_clusters.write(cluster+"\n")


