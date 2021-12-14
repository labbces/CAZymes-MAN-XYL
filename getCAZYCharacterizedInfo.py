import sys
import re
from os.path import exists, getmtime
from bs4 import BeautifulSoup
import urllib.request
import time
import argparse
from ete3 import NCBITaxa
# from DB.functions import *

# Initialize the NCBI taxonomy DB using ETE
ncbi = NCBITaxa()

parser = argparse.ArgumentParser(description='Download fdata from CAZy')
parser.add_argument('--family', dest='family', type=str, help='CAZy faimly')
parser.add_argument('--password', metavar='password',
                    type=str, help='password for the database')
parser.add_argument('--dropDBCAZyTables', dest='dropDBCAZyTables',
                    help='drop the tables related to CAZy web site from the database', default=False, action='store_true')
parser.add_argument('--loadDB', dest='loadDB',
                    help='load the database with CAZy website data', default=False, action='store_true')
parser.add_argument('--updateNCBITaxDB', dest='updateNCBITaxDB',
                    help='Update NCBI\'s taxonomy database', default=False, action='store_true')
# parser.add_argument('--typeFile', dest='typeFile', type=str, help='type fo page to downlaod, can be characterized or structure', default='structure', choices=['structure'])
args = parser.parse_args()

family = args.family.upper()
# typeFile= args.typeFile.lower()
url = f'http://www.cazy.org/{family}_characterized.html'  # url to download

infoFamily = {}
enzymes = []
fileInDisk = f'{family}_characterized.html'

if args.dropDBCAZyTables:
    if args.password:
        dropDBWebCAZyTables(args.password)
        createDB(args.password)
    else:
        print(f'if you want to perform any operation on the DB you must provide a password.', file=sys.stderr)
        sys.exit()

if exists(fileInDisk):
    print(
        f'Structure CAZY file for family {family} exists,  checking age', file=sys.stderr)
    mtime = getmtime(fileInDisk)
    now = time.time()
    if (now - mtime) > 604800:  # Download the file again if the file in disk is older than 7 days (60*60*24*7)
        print(
            f'Structure CAZY file for family {family}_characterized is older than 7 days, downloading', file=sys.stderr)
        urllib.request.urlretrieve(url, fileInDisk)
    else:
        print(
            f'Structure CAZY file for family {family}_characterized is younger than 7 days, not downloading and processing as it is', file=sys.stderr)
else:
    print(
        f'Structure CAZY file for family {family}_characterized does not exist,  start downloading', file=sys.stderr)
    urllib.request.urlretrieve(url, fileInDisk)
    print("Download complete", file=sys.stderr)

# Using local html to avoid problems with with cazy
with open(fileInDisk) as fhand:
    # Using BeatifulSoup to parse the html
    soup = BeautifulSoup(fhand, "lxml")
    table = soup.find("table", id="pos_onglet")
    for row in table.find_all('tr'):
        cols = row.find_all("td", id="separateur2")
        tdSubFam = row.find("td", id="separateur2", attrs={'align': 'center'})
        if len(cols) > 1:
            # proteinName is the first column
            proteinName = cols[0].text.strip()
            # Ignore the data coming from the PDB data
            '''if not re.match(r'[0-9A-Z]{4}\[[A-Z,]*\]', proteinName):
                data = {}
                subtable = row.find("table")'''
            data = {}
            if proteinName in data.keys():
                print(
                    f'{proteinName} is already in the dictionary, processing anyway.', file=sys.stderr)
#          continue #Canot do this, There are enzymes with duplicated names check GH1 in CAZY.org
            else:
                data[proteinName] = {}
                # Enzyme Code is in the second column
                ecCode = cols[1].text.strip()
                if ecCode:
                    data[proteinName]['ec'] = {}
                    if re.search(r'_or_', ecCode):
                        ecs = ecCode.split('_or_')
                        for ec in ecs:
                            data[proteinName]['ec'][ec] = 1
                    else:
                        data[proteinName]['ec'][ecCode] = 1
                # Reference is in the third column
                referenceLinks = cols[2].find_all('a')
                # print(referenceLinks)
                if referenceLinks:
                    data[proteinName]['References'] = {}
                    for link in referenceLinks:
                        # print(link.get('href'))
                        doi = re.search(
                            r'https://doi.org/(.*)', link.get('href'))
                        pubmed = re.search(
                            r'http://pubmed.ncbi.nlm.nih.gov/([0-9]*)', link.get('href'))
                        if pubmed:
                            data[proteinName]['References'][pubmed.group(
                                1)] = 'pubmed'
                        elif doi:
                            data[proteinName]['References'][doi.group(
                                1)] = 'doi'
                        else:
                            data[proteinName]['References'][link.get(
                                'href')] = 'other'
                    # print(data[proteinName]['References'])
                # Organism is in the fourth column
                taxName = cols[3].text.strip()
                taxLink = cols[3].find('a')
                # print(taxName)
                if not taxLink:  # There are some PDB resolution that is passing here, for GH2, not sure why, this handles it.
                    print(f'No link for {taxName}', file=sys.stderr)
                    continue
                matchLink = re.search(
                    r'http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi\?id=([0-9]*)', taxLink.get('href'))
                # data[proteinName]['taxID']={}
                data[proteinName]['taxNameAsIs'] = taxName
                if matchLink:
                    data[proteinName]['taxID'] = matchLink.group(1)
                    #print(f'{taxLink.get("href")} {matchLink.group(1)}')
                else:
                    name2taxID = ncbi.get_name_translator([taxName])
                    if name2taxID:
                        data[proteinName]['taxID'] = name2taxID[taxName][0]
                        # print(name2taxID[taxName][0])
                    else:
                        taxName = ' '.join(taxName.split(' ')[0:2])
                        name2taxID = ncbi.get_name_translator([taxName])
                        if name2taxID:
                            data[proteinName]['taxID'] = name2taxID[taxName][0]
                        else:
                            # Dealing with some fringe cases of species names, these appear to be wrong in CAZy.org
                            if taxName == 'Lacticaseibacillus plantarum':
                                data[proteinName]['taxID'] = '1590'
                            else:
                                print(
                                    f'{taxName} not found in taxonomy for protein {proteinName} in family {family}', file=sys.stderr)
                        #print(f'sss {taxName} {name2taxID}')
                # Get subfamily numver when available:
                subFamily = ''
                if(tdSubFam.text.strip() != '' and re.search(r'[0-9]*', tdSubFam.text.strip())):
                    # print(f'{proteinName}\tSubFam:{tdSubFam.text.strip()}')
                    subFamily = tdSubFam.text.strip()
                    data[proteinName]['subFamily'] = subFamily
                # Sequence accession are in the fifth column - for GenBank accession
                # A cell (td) can have multiple accession separeted by a <br>, with the methods stripped_strings I can get them and turn them into a list
                seqAccsGenkBank = list(cols[4].stripped_strings)
                if seqAccsGenkBank:
                    data[proteinName]['seqAccsGenBank'] = {}
                    for acc in seqAccsGenkBank:
                        data[proteinName]['seqAccsGenBank'][acc] = subFamily
                # Sequence accession are in the sixth column - for UniProt accession
                seqAccsUniprot = list(cols[5].stripped_strings)
                if seqAccsUniprot:
                    data[proteinName]['seqAccsUniprot'] = {}
                    for acc in seqAccsUniprot:
                        data[proteinName]['seqAccsUniprot'][acc] = subFamily
                # PDB accession are in the seventh column - PDB/3D
                PDBLinks = cols[6].find_all('a')
                # print(PDBLinks)
                data[proteinName]['PDB'] = {}
                if PDBLinks:
                    for link in PDBLinks:
                        # print(link.get('href'))
                        rcsb_link = re.search(
                            r'http://www.rcsb.org/pdb/explore/explore.do\?structureId=(.*)', link.get('href'))
                        if rcsb_link:
                            data[proteinName]['PDB'][rcsb_link.group(1)] = 1
                        else:
                            data[proteinName]['PDB'][link.get(
                                'href')] = 'non-PDB'
                # print(data[proteinName]['PDB'])
            enzymes.append(data)
    # print(enzymes)
    tableFamily = soup.find(
        "table", attrs={'cellspacing': '1', 'cellpadding': '1', 'border': '0'})
    # print(tableFamily)
    infoFamily[family] = {}
    for rowFam in tableFamily.find_all('tr'):
        rowHeader = rowFam.find('th', attrs={'class': "thsum"})
        rowData = rowFam.find("td", attrs={'class': "tdsum"})
        #rowData=rowData.text.strip().replace(" ?; ?", ";")
        rowData = re.sub(r' ?; ?', ';', rowData.text.strip())
        if rowHeader.text.strip() == 'Note' or rowHeader.text.strip() == 'External resources' or rowHeader.text.strip() == 'Statistics':
            continue
        infoFamily[family][rowHeader.text.strip()] = []
        for d in rowData.split(';'):
            if d == '':
                continue
            infoFamily[family][rowHeader.text.strip()].append(d)

if args.loadDB:
    updateNCBITaxDB = False
    if(args.updateNCBITaxDB):
        updateNCBITaxDB = True

    if args.password:
        populateWebCAZyInfo(password=args.password, updateNCBITaxDB=updateNCBITaxDB,
                            infoFamily=infoFamily, enzymes=enzymes, family=args.family)
    else:
        print(f'if you want to perform any operation on the DB you must provide a password.', file=sys.stderr)
        sys.exit()
