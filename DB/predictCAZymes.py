#Predict CAZymes
from functions import *
import argparse
parser= argparse.ArgumentParser(description='Download genomes from NCBI')
parser.add_argument('--password', metavar='password', type=str, help='password for the database', required=True)
parser.add_argument('--pathDir', dest='pathDir', type=str, help='Base path where Genome files were downloaded', required=True)
parser.add_argument('--maxGenomeID', dest='maxGenomeID', type=int, help='Max GenomeID of previous iterations, will start from this one')
args= parser.parse_args()

createDB(args.password)
submitCAZymeSearch(password=args.password,countIter=0,pathDir=args.pathDir,maxGenomeID=args.maxGenomeID)
#loadDbCANResults(password=args.password,pathDir=args.pathDir)
