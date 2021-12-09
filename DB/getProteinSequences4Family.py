#get Protein sequences for a given family
from functions import *
import argparse
parser= argparse.ArgumentParser(description='get Protein sequences for a given family')
parser.add_argument('--password', metavar='password', type=str, help='password for the database', required=True)
parser.add_argument('--family', dest='family', type=str, help='CAZy family', required=True)
args= parser.parse_args()

getProteinsFasta(password=args.password,familyID=args.family)
