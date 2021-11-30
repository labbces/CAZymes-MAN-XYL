from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database')
parser.add_argument('--apiKey', dest='apiKey', type=str, help='Entrez API key')
args= parser.parse_args()

updateProteinSequences(password=args.password, apiKey=args.apiKey)
