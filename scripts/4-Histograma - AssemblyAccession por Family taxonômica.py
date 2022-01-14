# imports
import re
import csv
import matplotlib.pyplot as plt
from Bio import SeqIO
from ete3 import NCBITaxa
import pandas as pd
import argparse
# pd.plotting.register_matplotlib_converters()

# Get desired taxonomica rank
parser = argparse.ArgumentParser()
parser.add_argument(
    "TaxonomicRank", help="Group, kingdom, phylum, class, order, family, genus, species")
args = parser.parse_args()

rank = args.TaxonomicRank

#
ncbi = NCBITaxa()

infoDict = {}
proteinsAverageDict = {}
noRankInfoAmount = 0
totalProteins = 0


with open("script.sh.o27551", "r") as ProteinSeqsFile:
    # Get ID line
    for line in ProteinSeqsFile:
        if line.startswith(">"):
            totalProteins += 1

# Get taxID, Group and Assembly Accession from ID line
            id4search = re.search(
                r'>(.*) AssemblyAccession:\[(.*)\];CazyFamily:\[(.*)\];taxID:\[(.*)\];name:\[(.*)\];species:\[(.*)\];Group:\[(.*)\]', line)
            assemblyAccession = str(id4search.group(2))
            taxID = str(id4search.group(4))
            group = str(id4search.group(7))
            ID = str(id4search.group(1))

# Get RankID
            if rank != "group":
                lineage = ncbi.get_lineage(taxID)
                lineage2ranks = ncbi.get_rank(lineage)
                # dict with TaxonomicRankID
                ranks2lineage = dict((rank, taxID)
                                     for (taxID, rank) in lineage2ranks.items())
                try:
                    taxRank = str(ranks2lineage[rank])
                except:
                    # print(f'Protein {ID} does not have {rank} information: {ranks2lineage}')
                    noRankInfoAmount += 1
                    pass
            else:
                taxRank = group

# Building information dict (protein redundancies by protein, by Assembly Accession, by desired RankID)
        else:
            seq = line
            if taxRank not in infoDict.keys():
                infoDict[taxRank] = {}
            if assemblyAccession not in infoDict[taxRank].keys():
                infoDict[taxRank][assemblyAccession] = {}
            if seq not in infoDict[taxRank][assemblyAccession].keys():
                infoDict[taxRank][assemblyAccession][seq] = 1
            else:
                infoDict[taxRank][assemblyAccession][seq] += 1

print(f"{noRankInfoAmount} proteins does not have rank information in a total of {totalProteins} proteins or {(noRankInfoAmount*100)/totalProteins}%")

# Building average
averageDict = {}
for rankID in infoDict.keys():
    sum = 0
    assemblyAccessionAmount = len(infoDict[rankID])
    # print(f'Assembly Accession Amount {assemblyAccessionAmount}')

    for assemblyAccession in infoDict[rankID].keys():
        for protein in infoDict[rankID][assemblyAccession]:
            sum += infoDict[rankID][assemblyAccession][protein]

    average = sum/assemblyAccessionAmount
    #print(f'Sum {sum}')
    #print(f'Assembly Accesion Amount {assemblyAccessionAmount}')
    #print(f'For RankID {rankID} the average of proteins by assembly accession is {average}')

    averageDict[rankID] = average
print(averageDict)

# Create the grph
df = pd.DataFrame(averageDict.items(), columns=[
                  'Rank ID', 'average']).sort_values('average', ascending=False)
df.hist(column='average', bins=100, ylabelsize=1)

plt.savefig(fname='TestFig.png', dpi='figure', format='png')
