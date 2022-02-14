# imports
from distutils.log import info
import re
import csv
import matplotlib.pyplot as plt
from Bio import SeqIO
from ete3 import NCBITaxa
import pandas as pd
import argparse
import numpy as np

# Get desired taxonomic rank
parser = argparse.ArgumentParser()
parser.add_argument(
    "ProteinsFile", help="file that contains protein sequences per family")
parser.add_argument(
    "TaxonomicRank", help="group, kingdom, phylum, class, order, family, genus, species")
args = parser.parse_args()

rank = args.TaxonomicRank
ptn_file = args.ProteinsFile

# Build info dict: proteins amount per rank ID and assembly accession
ncbi = NCBITaxa()

infoDict = {}
proteinsAverageDict = {}
noRankInfoAmount = 0
totalProteins = 0


with open(ptn_file, "r") as ProteinSeqsFile:
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
                    rankID = str(ranks2lineage[rank])
                except:
                    # print(f'Protein {ID} does not have {rank} information: {ranks2lineage}')
                    noRankInfoAmount += 1
                    pass
            else:
                rankID = group

            if rankID not in infoDict.keys():
                infoDict[rankID] = {}
            if assemblyAccession not in infoDict[rankID].keys():
                infoDict[rankID][assemblyAccession] = 1
            else:
                infoDict[rankID][assemblyAccession] += 1

# print(infoDict)
print(f"{noRankInfoAmount} proteins does not have rank information in a total of {totalProteins} proteins or {(noRankInfoAmount*100)/totalProteins}%")

data = []
labels = []
for rankID in infoDict.keys():
    sub_data = list(infoDict[rankID].values())
    data.append(sub_data)

    if rank != "group":
        lineage = ncbi.get_lineage(rankID)
        names = ncbi.get_taxid_translator(lineage)
        try:
            rank_name = names[int(rankID)]
            labels.append(rank_name)
        except:
            labels.append(rankID)
            print(f'RankID {rankID} does not have a defined name')
    else:
        labels.append(rankID)

# print(data)
# print(labels)

# Building plots
data = data
fs = 10
n = 1
pos = []
while n <= len(data):
    pos.append(n)
    n += 1


def plot_format(xlabel, ylabel, title, pos, labels, rotation, grid, label_fontsize, title_fontsize, xlabel_fontsize):
    plt.xlabel(str(xlabel), fontsize=label_fontsize)
    plt.ylabel(str(ylabel), fontsize=label_fontsize)
    plt.title(str(title), fontsize=title_fontsize)
    plt.xticks(pos, labels, rotation=rotation, fontsize=xlabel_fontsize)
    plt.grid(grid)
    plt.tight_layout()


# Building violin plots
plt.figure()
plt.violinplot(data, pos, widths=0.7,
               showmeans=True, showextrema=True, showmedians=True)
plot_format('Taxonomic Rank', 'Proteins Amount', title=str(id4search.group(3)), pos=pos,
            labels=labels, rotation=90, grid=True, label_fontsize=15, title_fontsize=17, xlabel_fontsize=10)


fig_name = f'{str(id4search.group(3))}_{rank}_AssemblyAccessionPerTaxonomicRank_ViolinPlot.png'
plt.savefig(fname=fig_name, dpi='figure', format='png')

# Building Box plots
plt.figure()
plt.boxplot(data, positions=pos)
plot_format('Taxonomic Rank', 'Proteins Amount', title=str(id4search.group(3)), pos=pos,
            labels=labels, rotation=90, grid=True, label_fontsize=15, title_fontsize=17, xlabel_fontsize=10)

fig_name = f'{str(id4search.group(3))}_{rank}_AssemblyAccessionPerTaxonomicRank_BoxPlot.png'
plt.savefig(fname=fig_name, dpi='figure', format='png')
