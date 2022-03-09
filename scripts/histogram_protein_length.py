import argparse
from Bio import SeqIO
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt

# Using argparse to handle variables
parser = argparse.ArgumentParser()
parser.add_argument("infile", help= "predicted cazymes file obtaineid from dbCAN result")
parser.add_argument("outfile", help="name of the outfile histogram")
parser.add_argument("nbins", type=int,help="number of bins")
args = parser.parse_args()

try:
    handle = open(args.infile, "r")
except:
    print("File",args.infile,"cannot be opened")
    exit()
    
png_name = args.outfile
num_of_bins = args.nbins

# Making Dic/Histogram
histogram = dict()

for seq_record in SeqIO.parse(handle,"fasta"):
    amino_acid_number = len(seq_record.seq)
    histogram[amino_acid_number] = histogram.get(amino_acid_number,0) + 1

# Making DataFrame
df = pd.DataFrame(histogram.items(), columns=['Sequence_length','Repetitions']).sort_values('Repetitions',ascending=False)

# Ploting the Histogram
df.hist(column='Sequence_length', bins=num_of_bins)

plt.xlabel("Lenghth of Sequence", size = 16)
plt.ylabel("Number ofsequences", size = 16)
plt.title("Representation of the length of Sequences", 
          fontdict={'family': 'serif', 
                    'color' : 'black',
                    'weight': 'bold',
                    'size': 18})

plt.savefig(f'{png_name}.png')
plt.show()
