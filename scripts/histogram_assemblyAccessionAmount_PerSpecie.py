# imports
import matplotlib.pyplot as plt
from Bio import SeqIO
import argparse
import re
import pandas as pd
pd.plotting.register_matplotlib_converters()

# Get proteins file
parser = argparse.ArgumentParser()
parser.add_argument(
    "ProteinsFile", help="file that contains protein sequences per family")
args = parser.parse_args()

ptn_file = args.ProteinsFile

# Creating dictionary {specie:[assemblyAcessions]}
assembly_accession = []
species = []
with open(ptn_file, "r") as ProteinSeqsFile:
    AssemblyAcessionAmount = {}
    for line in ProteinSeqsFile:
        if line.startswith(">"):
            id4search = re.search(
                r'>(.*) AssemblyAccession:\[(.*)\];CazyFamily:\[(.*)\];taxID:\[(.*)\];name:\[(.*)\];species:\[(.*)\];Group:\[(.*)\]', line)
            assemblyAccessionID = str(id4search.group(2))
            specie = str(id4search.group(6))

            if assemblyAccessionID not in assembly_accession:
                assembly_accession.append(assemblyAccessionID)

            if specie not in species:
                species.append(specie)

            if specie in AssemblyAcessionAmount.keys():
                if assemblyAccessionID not in AssemblyAcessionAmount[specie]:
                    AssemblyAcessionAmount[specie].append(assemblyAccessionID)
            else:
                AssemblyAcessionAmount[specie] = [assemblyAccessionID]


# print(AssemblyAcessionAmount)
print(f'There are {len(assembly_accession)} unique assembly accessions for the {str(id4search.group(3))} family')

# Converting dictionary to {specie:[assemblyAcessions Amount]}
for specie in AssemblyAcessionAmount:
    length = len(AssemblyAcessionAmount[specie])
    AssemblyAcessionAmount[specie] = length

# print(AssemblyAcessionAmount)

# Data frame
df = pd.DataFrame(AssemblyAcessionAmount.items(), columns=[
                  'Species', 'AssemblyAccession Amount']).sort_values('AssemblyAccession Amount', ascending=True)

# Defining bin
bin = df['AssemblyAccession Amount'].iloc[-1]

# Building histogram
df.hist(column='AssemblyAccession Amount', bins=bin)

plt.ylabel("Species", size=14,)
plt.xlabel("Assembly Accession Amount", size=14)
title = f'Assembly Accession Amount per specie - {str(id4search.group(3))}'
plt.title(label=title,
          fontdict={'family': 'serif',
                    'color': 'black',
                    'size': 14})

fig_name = f'{str(id4search.group(3))}_AssemblyAccessionDistribuition.png'
plt.savefig(fname=fig_name, dpi='figure', format='png')
