import argparse
from pathlib import Path    
from collections import Counter
from statistics import mean
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 20)

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument("--input",help="clstr output file from cd-hit",type=str,required=True)
parser.add_argument("--output",help="prefix of output file",type=str,required=True)
parser.add_argument("--family",help="facilitates identification in stdout",type=str)
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
output = args.output

with open(clstr, "r") as parser:
    hash = dict()
    hash2 = dict()
    cluster = 0
    for line in parser:
        if line.startswith(">"):
            cluster += 1
        else:
            line1 = line.split(">")
            hash[line1[1]] = cluster
            line2 = line.split("\t")[1].split("aa")[0]
            try:
                hash2[cluster].append(int(line2))
            except:
                hash2[cluster] = list()
                hash2[cluster].append(int(line2))

# Registering the clusters that have studied sequences
studied = list()

for key in hash.keys():
    if "characterized" in key: 
            studied.append(hash[key])
    elif "structure" in key:
            studied.append(hash[key])

# Doing the count of cluster's sequences in counter library
contagem = Counter(hash.values())


cluster_id, clength, seq_average, largest_seq, smallest_seq = list(),list(),list(),list(),list()  # setting variables

# Separating studied and not studied clusters
not_studied_cluster = 0 
if args.family:
    family = args.family
    print(f'Clusters without studies proteins in {family}:')
else:
    print('Clusters without studies proteins:')
for number in range(1,cluster):
    if number not in studied:
        not_studied_cluster += 1
        print( f'''Cluster:{number -1}; Cluster's length:{contagem[number]}; Sequences average length:{round(mean(hash2[number]))}; Largest sequence:{max(hash2[number])}; Shortest sequence:{min(hash2[number])}''')
        cluster_id.append(number - 1), clength.append(contagem[number]), seq_average.append(round(mean(hash2[number]))), largest_seq.append(max(hash2[number])), smallest_seq.append(min(hash2[number]))

print(f'Relationship of not studied and studied cluster:{not_studied_cluster}/{cluster}; {round(not_studied_cluster/cluster,2)}')

# Creating a csv file with the information
tabela = pd.DataFrame({'Sequences': [id.split("+")[0]for id in hash.keys()], 'Cluster': [value -1 for value in hash.values()]})
tabela.to_csv(f'{path}/shortened-seq-cluster_{family}.csv', header=True, index=False, sep="\t")
tabela2 = pd.DataFrame({'Sequences': list(hash.keys()), 'Cluster': [value -1 for value in hash.values()]})
tabela2.to_csv(f'{path}/complete-seq-cluster_{family}.csv', header=True, index=False, sep="\t")
tabela3 = pd.DataFrame({'cluster_id': cluster_id, 'clenth': clength, 'seq-average': seq_average, 'seq-largest': largest_seq, 'seq-smallest': smallest_seq})
tabela3.to_csv(f'{path}/metadata_{family}.csv', header=True, index=False, sep="\t")