# IMPORTS
import argparse
from calendar import c
import csv
from Bio import SeqIO

# Using argparse to handle variables
parser = argparse.ArgumentParser()
#parser.add_argument("-c", "--clustersinfo", help="Clusters informations. GHX_NN.clstr Output cd-hit", type=str, required=True)
parser.add_argument("-c", "--clustersinfo",
                    help="shortened-seq-cluster_GHX output of hc_parcing.py", type=str, required=True)
parser.add_argument("-f", "--predictedFasta",
                    help="Fasta with predicted sequences", type=str, required=True)
parser.add_argument("-d", "--ECcodeInfo",
                    help="GHXX_ECcode_detailed output from", type=str, required=True)
parser.add_argument("-p", "--prefixFilename",
                    help="prefix name for output files", type=str, required=True)
args = parser.parse_args()

# Identifying predicted interesting clusters
clusterStatus = []
cluster = ''
studied = False
with open(args.ECcodeInfo, 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        # print(row)
        if row[0] != "Cluster":
            cluster_new = row[0]
            status = row[3]
            if cluster_new == cluster:
                if status != 'Predicted':
                    studied = True
            else:
                if cluster != '':
                    if studied == False:
                        try:
                            clusterStatus.append(cluster)
                        except:
                            print('Something is not correct')
                    studied = False
            cluster = cluster_new

print(clusterStatus)

clusterID = {}
with open(args.clustersinfo, 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter='\t')
    for row in csv_reader:
        cluster = row[1]
        if cluster in clusterStatus:
            try:
                clusterID[cluster].append(row[0])
            except:
                clusterID[cluster] = []
                clusterID[cluster].append(row[0])

print(clusterID)

seqs = {}
for cluster in clusterID.keys():
    with open(args.predictedFasta) as handle:
        for record in SeqIO.parse(handle, "fasta"):
            if record.id in clusterID[cluster]:
                try:
                    seqs[cluster].append(record)
                except:
                    seqs[cluster] = []
                    seqs[cluster].append(record)
                # print(record.id)
                # print(record.seq)

for cluster in clusterID.keys():
    filenam = f'{args.prefixFilename}_cluster{cluster}.fasta'
    SeqIO.write(seqs[cluster], filenam, "fasta")
