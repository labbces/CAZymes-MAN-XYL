from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database')
parser.add_argument('--dropDB', dest='dropDB', help='drop the database', default=False, action='store_true')
parser.add_argument('--updateNCBITaxDB', dest='updateNCBITaxDB', help='Update NCBI\'s taxonomy database', default=False, action='store_true')
parser.add_argument('--typeOrg', dest='typeOrg', type=str, help='Can be prok or euk', default='euk', choices=['euk', 'prok'])
args= parser.parse_args()

updateNCBITaxDB=False

if(args.dropDB):
    dropDB(args.password)
    createDB(args.password)

if(args.updateNCBITaxDB):
    updateNCBITaxDB=True

#populateGenomes('https://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/eukaryotes.txt',password=args.password,typeOrg=args.typeOrg,updateNCBITaxDB=updateNCBITaxDB)
populateGenomes(url='https://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/prokaryotes.txt',password=args.password,typeOrg=args.typeOrg,updateNCBITaxDB=updateNCBITaxDB)