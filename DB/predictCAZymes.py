#Predict CAZymes
from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database')
args= parser.parse_args()

createDB(args.password)
predictCAZymes(password=args.password)
