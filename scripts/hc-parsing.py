import argparse
from collections import Counter
from statistics import mean

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument("clstr",help="clstr files with cd-hit clusters")
args = parser.parse_args()

# Setting Variables
clstr = args.clstr

# Reading files safely
try:
    seq_handler = open(args.clstr, 'r')
    seq_handler.close
except:
    print("File", args.clstr, "cannot be open")
    exit()

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

          
count = 0
studied = list()

for key in hash.keys():
    if "characterized" in key: 
            studied.append(hash[key])

for key in hash.keys():
    if "structure" in key: 
            studied.append(hash[key]) 

contagem = Counter(hash.values())

not_studied_cluster = 0
print("Clusters without studies proteins:")
for number in range(1,cluster):
    if number not in studied:
        not_studied_cluster += 1
        print( f'''Cluster:{number -1}; Cluster's length:{contagem[number]}; Sequences average length:{round(mean(hash2[number]))}; Largest sequence:{max(hash2[number])}; Shortest sequence:{min(hash2[number])}''')

print(f'Relationship of not studied and studied cluster:{not_studied_cluster}/{cluster}; {round(not_studied_cluster/cluster,2)}')
