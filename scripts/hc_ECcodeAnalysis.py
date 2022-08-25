import argparse
from pathlib import Path
import re
import tarfile
import pandas as pd
import csv

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help="clstr output file from cd-hit hierarchical clustering", type=str, required=True)
parser.add_argument("-s","--studiedECs", help="file with EC codes of studied sequences",type=str, required=True)
parser.add_argument("-o","--output", help="path to write file", type=str, required=True)
parser.add_argument("-p","--predictedECs", help="file with EC codes of predicted sequences",type=str, required=True)
parser.add_argument("-ic","--interestECs", help="file with EC codes of interest",type=str, required=True)
parser.add_argument("-f","--family", help="prefix for filename", type=str, required=True)

args = parser.parse_args()


# Writing path if don't exist already
path = Path(args.output)
path.mkdir(exist_ok=True)

# Reading files safely
try:
    seq_handler = open(args.input, 'r')
    seq_handler.close
except:
    print("File", args.input, "cannot be open")
    exit()

# Setting Variables
clstr = args.input


with open(clstr, "r") as parser:
    seqs = {}
    cluster_rep_seqs = {}
    cluster = 0
    for line in parser:
        if line.startswith(">"):
            cluster += 1
            seqs[str(cluster)] = []
        else:
            line1 = line.split(">")[1].split(";")[0]
            seqs[str(cluster)].append(line1)
            if line.strip().endswith('*'):
                cluster_rep_seqs[cluster] = line1.split('+')[0]

# print(seqs)
# print(cluster_rep_seqs)

# Registering the clusters that have studied sequences
studied_info = {}

for cluster in seqs.keys():
    total = 0
    total_studied = 0
    for seq in seqs[cluster]:
        total += 1
        if 'structure' in seq:
            total_studied += 1
        if 'characterized' in seq:
            total_studied += 1
    ratio = total_studied*100/total
    studied_info[cluster] = {'Total': total,
                             'Total Studied': total_studied,
                             'Ratio': ratio}
# print(studied_info)

interestECs = args.interestECs
with open(interestECs, 'r') as interestECs:
    interestEC = {}
    for line in interestECs:
        EC_code = line.split(';')[1].replace('"', '')
        bond = line.split(';')[2]
        substrate = line.split(';')[4].replace('\n', '')
        try:
            interestEC[EC_code].append(substrate)
            interestEC[EC_code].append(bond)
        except:
            interestEC[EC_code] = []
            interestEC[EC_code].append(substrate)
            interestEC[EC_code].append(bond)
# print(interestEC)

studiedEC_file = args.studiedECs
with open(studiedEC_file, 'r') as studiedECs_file:
    studiedEC = {}
    for line in studiedECs_file:
        if args.family in line:
            EC_code = line.split(';')[0]
            if EC_code in interestEC.keys():
                seq_ID = line.split(';')[1]
                try:
                    studiedEC[seq_ID].append(EC_code)
                except:
                    studiedEC[seq_ID] = []
                    studiedEC[seq_ID].append(EC_code)
# print(studiedEC)


predictedEC_file = args.predictedECs

with open(predictedEC_file, 'r') as predictedEC_file:
    predictedEC = {}
    for line in predictedEC_file:
        if args.family in line:
            EC_code = line.split('\t')[1].split(':')[1].replace('\n', '')
            if EC_code in interestEC.keys():
                seq_ID = line.split('\t')[0].split('+')[0]
                try:
                    predictedEC[seq_ID].append(EC_code)
                except:
                    predictedEC[seq_ID] = []
                    predictedEC[seq_ID].append(EC_code)
# print(predictedEC)

final_results = []
resultado_detalhado2 = []

for cluster in seqs.keys():
    cluster_info = {}
    EC_total = 0
    EC_predicted = 0
    EC_studied = 0
    ids = []
    substrate = []
    for seq in seqs[cluster]:
        resultado_detalhado = {}
        seq_ID = seq.split('+')[0]
        if seq_ID not in ids:
            ids.append(seq_ID)
            if seq_ID in studiedEC.keys():
                EC_studied += 1
                EC_total += 1
                EC_code = studiedEC[seq_ID]

                resultado_detalhado['Cluster'] = cluster
                resultado_detalhado['seqID'] = seq_ID
                resultado_detalhado['EC code'] = EC_code[0]
                resultado_detalhado['EC code type'] = 'Studied'
                resultado_detalhado['Bond'] = interestEC[EC_code[0]][1]
                resultado_detalhado['Substrate'] = interestEC[EC_code[0]][0]

                resultado_detalhado2.append(resultado_detalhado)
                resultado_detalhado = {}

                if interestEC[EC_code[0]][0] not in substrate:
                    substrate.append(interestEC[EC_code[0]][0])

            elif seq_ID in predictedEC.keys():
                EC_predicted += 1
                EC_total += 1
                EC_code = predictedEC[seq_ID]

                resultado_detalhado['Cluster'] = cluster
                resultado_detalhado['seqID'] = seq_ID
                resultado_detalhado['EC code'] = EC_code[0]
                resultado_detalhado['EC code type'] = 'Predicted'
                resultado_detalhado['Bond'] = interestEC[EC_code[0]][1]
                resultado_detalhado['Substrate'] = interestEC[EC_code[0]][0]

                resultado_detalhado2.append(resultado_detalhado)
                resultado_detalhado = {}

                if interestEC[EC_code[0]][0] not in substrate:
                    substrate.append(interestEC[EC_code[0]][0])

    if EC_total > 0:
        cluster_info['Cluster'] = cluster
        cluster_info['Representative sequence ID'] = cluster_rep_seqs[int(
            cluster)]
        cluster_info['Total EC code'] = EC_total
        cluster_info['Predicted EC code amount'] = EC_predicted
        cluster_info['Studied EC code amount'] = EC_studied
        cluster_info['Substrate'] = substrate
        cluster_info['Studied Ratio'] = str(
            studied_info[cluster]['Ratio']).replace('.', ',')
        cluster_info['Total sequences amount'] = studied_info[cluster]['Total']
        cluster_info['Total studied sequences amount'] = studied_info[cluster]['Total Studied']

        # print(cluster_info)
        final_results.append(cluster_info)
# print(resultado_detalhado2)
# print(final_results)

df = pd.DataFrame.from_dict(resultado_detalhado2)
name = f'{args.family}_ECcode_detailed.csv'
df.to_csv(f'{args.output}/name', index=False, header=True)

df = pd.DataFrame.from_dict(final_results)
name2 = f'{args.family}_clusterInfo.csv'
df.to_csv(f'{args.output}/name2', index=False, header=True)
