from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database')
parser.add_argument('--dropDB', dest='dropDB', help='drop the database', default=False, action='store_true')
parser.add_argument('--updateNCBITaxDB', dest='updateNCBITaxDB', help='Update NCBI\'s taxonomy database', default=False, action='store_true')
args= parser.parse_args()

updateNCBITaxDB=False

if(args.dropDB):
    dropDB(args.password)

if(args.updateNCBITaxDB):
    updateNCBITaxDB=True

createDB(args.password)
populateGenomes('https://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/eukaryotes.txt',args.password,updateNCBITaxDB)
# populateGenomes('https://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/prokaryotes.txt',args.password)
