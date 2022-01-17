import argparse
from Bio import SeqIO
from random import sample 

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument("cazy_family",help="file with cazy family")
parser.add_argument("outputfile",help="name of the output file")
parser.add_argument("reporting_family",help="name of the used name to be tracked in the report file")
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
report_family = args.reporting_family

# Sorting algorithm, for now, using replace.py to change replaces for underlines
# First loop to get assemblies ids

assembly_acession = []

with open(cazy,"r") as handle:
    for seqs in SeqIO.parse(handle, "fasta"):
        try:
            assembly_id = seqs.id.split("[")[1].split("]")[0]
            if assembly_id not in assembly_acession:
                assembly_acession.append(assembly_id)
        except:
            print('Parsing error')

choosen_assemblies = sample(assembly_acession,k=2)

# Second loop to make a list only with select assemblies
new_multi_fasta = list()

with open(cazy,"r") as handle:
    for seqs in SeqIO.parse(handle, "fasta"):
        try:
            assembly_id = seqs.id.split("[")[1].split("]")[0]
            if assembly_id not in choosen_assemblies: continue
            new_multi_fasta.append(seqs)
        except:
            print('Parsing error')

 # Writing new file with sampled assemblies 
SeqIO.write(new_multi_fasta,outfile,"fasta")

# Writing new file informing id from sampled assemblies
with open(f"{report_family}.txt","w") as f:
    f.write(f"{report_family}, file with sampled assembly acession used: \n")
    for element in choosen_assemblies: 
        f.write(element + "\n")
