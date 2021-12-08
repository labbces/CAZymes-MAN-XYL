from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database')
parser.add_argument('--dirPath', dest='dirPath', type=str, help='folder where genome files shoudl be stored')
parser.add_argument('--fileType', dest='fileType', type=str, 
                    help='Which type of genome file should be downloaded',
                    choices=['Genome sequence', 'Protein sequence', 'Genome annotation', 'Protein sequence alter'])
args= parser.parse_args()

createDB(args.password)
downloadGenomeFiles(password=args.password, dirPath=args.dirPath, fileType=args.fileType)
