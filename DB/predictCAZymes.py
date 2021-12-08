#Predict CAZymes
from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database')
parser.add_argument('--pathDir', dest='pathDir', type=str, help='Base path where Genome files were downloaded')
args= parser.parse_args()

#createDB(args.password)
#submitCAZymeSearch(password=args.password,countIter=0,pathDir=args.pathDir)
loadDbCANResults(password=args.password,pathDir=args.pathDir)
