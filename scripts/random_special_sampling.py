import argparse
from Bio import SeqIO
from random import sample

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument("cazy_family",help="file with cazy family")
parser.add_argument("outputfile",help="name of the output file")
parser.add_argument("report",help="name of the used assembly acession and family to be tracked in the report file")
parser.add_argument("sampling_percentage",type=int, help="Percentage of the total of sequences that should be sampled")
args = parser.parse_args()

# Reading files safely
try:
    seq_handler = open(args.cazy_family, 'r')
    seq_handler.close()
except:
    print("File", args.cazy_family, "cannot be open")
    exit()

# Setting varibles
cazy = args.cazy_family  
outfile = args.outputfile
report = args.report
sampling_percentage = int(args.sampling_percentage)

# Sorting algorithm
# 1) First loop to get assemblies ids   # Trocar [parse] por [index]

assembly_acession = []
seqs_count = 0

with open(cazy,"r") as handle:
    for seqs in SeqIO.parse(handle, "fasta"): # Biopython 
        try:
            seqs_count += 1
            assembly_id = seqs.description.split("[")[2].split("]")[0]  # Modificar para deixar mais legivel
            print(assembly_id)
            if assembly_id not in assembly_acession:
                assembly_acession.append(assembly_id)
        except:
            print('Parsing error')

# Doing the sampling
percentage = sampling_percentage / 100
n_of_assemblies = round(len(assembly_acession) * percentage) 
choosen_assemblies = sample(assembly_acession, n_of_assemblies)

# 2) Second loop to make a list only with select assemblies 
new_multi_fasta = list()

with open(cazy,"r") as handle:
    for seqs in SeqIO.parse(handle, "fasta"):
        try:
            assembly_id = seqs.description.split("[")[2].split("]")[0]
            if assembly_id not in choosen_assemblies: continue
            new_multi_fasta.append(seqs)
        except:
            print('Parsing error')

 # Writing new file with sampled assemblies 
SeqIO.write(new_multi_fasta,outfile,"fasta")

# Writing new file informing id from sampled assemblies
with open(f"{report}.txt","w") as f:
    f.write(f"{report}, Assembly acession used in the sampling: \n")
    for element in choosen_assemblies: 
        f.write(element + "\n")
