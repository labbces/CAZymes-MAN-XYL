import sys
import re
from os.path import exists, getmtime
from bs4 import BeautifulSoup
import urllib.request
import time
import argparse
from ete3 import NCBITaxa

#CAZymes classes
CAZymeClasses={
  'GH':'Glycoside Hydrolases',
  'GT':'GlycosylTransferases',
  'PL':'Polysaccharide Lyases',
  'CE':'Carbohydrate Esterases',
  'AA':'Auxiliary Activities',
  'CBM':'Carbohydrate Binding Modules'
}

#Initialize the NCBI taxonomy DB using ETE
ncbi = NCBITaxa()

parser= argparse.ArgumentParser(description='Download fdata from CAZy')
parser.add_argument('--family', dest='family', type=str, help='CAZy faimly')
parser.add_argument('--typeFile', dest='typeFile', type=str, help='type fo page to downlaod, can be characterized or structure', default='structure', choices=['characterized', 'structure'])
args= parser.parse_args()

family= args.family.upper()
typeFile= args.typeFile.lower()
url = f'http://www.cazy.org/{family}_{typeFile}.html'	# url to download

data={}
infoFamily={}
fileInDisk= f'{family}_{typeFile}.html'

if exists(fileInDisk):
  print(f'Structure CAZY file for family {family} exists,  checking age', file=sys.stderr)
  mtime=getmtime(fileInDisk)
  now=time.time()
  if (now - mtime) > 604800: #Download the file again if the file in disk is older than 7 days (60*60*24*7)
    print(f'Structure CAZY file for family {family}_{typeFile} is older than 7 days, downloading', file=sys.stderr)
    urllib.request.urlretrieve(url, fileInDisk)
  else:
    print(f'Structure CAZY file for family {family}_{typeFile} is younger than 7 days, not downloading and processing as it is', file=sys.stderr)
else:
  print(f'Structure CAZY file for family {family}_{typeFile} does not exist,  start downloading', file=sys.stderr)
  urllib.request.urlretrieve(url, fileInDisk)
  print("Download complete", file=sys.stderr)
  
# Using local html to avoid problems with with cazy 
with open(fileInDisk) as fhand:
  # Using BeatifulSoup to parse the html
  soup = BeautifulSoup(fhand, "lxml")
  table = soup.find("table", id="pos_onglet")
  for row in table.find_all('tr'):
    cols=row.find_all("td", id="separateur2")
    if len(cols) >1:
      #proteinName is the first column
      proteinName=cols[0].text.strip()
      if not re.match(r'[0-9A-Z]{4}\[[A-Z,]\]*', proteinName):#Ignore the data coming from the PDB data
        subtable=row.find("table")
        if proteinName in data.keys():
          print(f'{proteinName} is already in the dictionary, skipping', file=sys.stderr)
          continue
        else:
          data[proteinName]={}
          #Enzyme Code is in the second column
          ecCode=cols[1].text.strip()
          if ecCode:
            data[proteinName]['ec']={}
            if re.search(r'_or_', ecCode):
              ecs=ecCode.split('_or_')
              for ec in ecs:
                data[proteinName]['ec'][ec]=1
            else:
              data[proteinName]['ec'][ecCode]=1
          #Organism is in the third column
          taxName=cols[2].text.strip()
          taxLink=cols[2].find('a')
          matchLink=re.search(r'http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi\?id=([0-9]*)', taxLink.get('href'))
          data[proteinName]['taxID']={}
          if matchLink:
            data[proteinName]['taxID']=matchLink.group(1)
            #print(f'{taxLink.get("href")} {matchLink.group(1)}')
          else:
            name2taxID=ncbi.get_name_translator([taxName])
            if name2taxID:
              data[proteinName]['taxID']=name2taxID[taxName]
              # print(name2taxID[taxName][0])
            else:
              taxName=' '.join(taxName.split(' ')[0:2])
              name2taxID=ncbi.get_name_translator([taxName])
              if name2taxID:
                data[proteinName]['taxID']=name2taxID[taxName][0]
              else:
                print(f'{taxName} not found in taxonomy for protein {proteinName} in family {family}')
              #print(f'sss {taxName} {name2taxID}')
          #Sequence accession are in the fourth column - for GenBank accession
          #A cell (td) can have multiple accession separeted by a <br>, with the methods stripped_strings I can get them and turn them into a list
          seqAccsGenkBank=list(cols[3].stripped_strings)
          if seqAccsGenkBank:
            data[proteinName]['seqAccsGenBank']={}
            for acc in seqAccsGenkBank:
              data[proteinName]['seqAccsGenBank'][acc]=1
          #Sequence accession are in the fifth column - for UniProt accession
          seqAccsUniprot=list(cols[4].stripped_strings)
          if seqAccsUniprot:
            data[proteinName]['seqAccsUniprot']={}
            for acc in seqAccsUniprot:
              data[proteinName]['seqAccsUniprot'][acc]=1
        #Get PDB accession form subtable
        data[proteinName]['PDB']={}
        for rows in subtable.find_all('tr'):
          cols=rows.find_all("td", id="separateur2")
          if len(cols) >1:
            #Getting PDB ids
            pdbAcc=cols[0].text.strip()
            match=re.search(r'([0-9A-Z]{4})\[([0-9A-Z,]+)\]', pdbAcc)
            if match:
              data[proteinName]['PDB'][match.group(1)]=match.group(2)
              #print(f'{match.group(1)} , {match.group(2)}')
            else:
              print(f'{pdbAcc} does not have a know string format for protein {proteinName} in family {family}')
  tableFamily = soup.find("table", attrs={'cellspacing':'1', 'cellpadding':'1', 'border':'0'})
  # print(tableFamily)
  infoFamily[family]={}
  for rowFam in tableFamily.find_all('tr'):
    rowHeader=rowFam.find('th', attrs={'class':"thsum"})
    rowData=rowFam.find("td", attrs={'class':"tdsum"})
    #rowData=rowData.text.strip().replace(" ?; ?", ";")
    rowData=re.sub(r' ?; ?',';',rowData.text.strip())
    if rowHeader.text.strip() == 'Note' or rowHeader.text.strip() == 'External resources' or rowHeader.text.strip() == 'Statistics':
      continue
    infoFamily[family][rowHeader.text.strip()]=[]
    for d in rowData.split(';'):
      if d=='':
        continue
      infoFamily[family][rowHeader.text.strip()].append(d)

for fam in infoFamily.keys():
  for key in infoFamily[fam].keys():
    for value in infoFamily[fam][key]:
      True
      #print(f'{fam}:{key}:{value}')

for pN in data.keys():
  if 'ec' in data[pN].keys():
    for ec in data[pN]['ec']:
      True
      # print(f'{pN} {ec}')


  # print(table.text)
  # rows = table.find_all("tr", onmouseover="this.bgColor='#F8FFD5';")
  # table_with_pdb = soup.find_all("table", border="0", width='100%')

  # protein_column = list()
  # EC_column = list()
  # EC_link_column = list()
  # Organism_column = list()
  # Organism_link_column = list()
  # Genbank_column = list()
  # Genbank_link_column = list()
  # Uniprot_column = list()
  # Uniprot_link_column = list()

  # for row in rows:
  #       protein_name = row.find("td", id="separateur2").text
  #       protein_column.append(protein_name)
  # for row in rows:
  #       EC = row.find_all("td")[1].text
  #       if EC == "":
  #         EC_column.append(NA)
  #       else:
  #         EC_column.append(EC)
  #       try:
  #         EC_link = row.find_all("td")[1].a['href']
  #         EC_link_column.append(EC_link)
  #       except:
  #         EC_link_column.append("NaN")
  # for row in rows:
  #       try:
  #         Organism = row.find_all("td")[2].text.strip()
  #         Organism_column.append(Organism)
  #         Organism_link = row.find_all("td")[2].a['href']
  #         Organism_link_column.append(Organism_link)
  #       except:
  #         Organism_column.append("NaN")
  #         Organism_column_link.append("NaN")

  # for row in rows:
  #         Genbank = row.find_all("td")[3].get_text(strip=True)
  #         Genbank_column.append(Genbank)

  # for row in rows:
  #         try:
  #           Genbank_link = row.find_all("td")[3].a['href']
  #           Genbank_link_column.append(Genbank_link)
  #         except:
  #           Genbank_link_column.append("NaN")
            
  # for row in rows:
  #         Uniprot = row.find_all("td")[4].get_text(strip=True)
  #         Uniprot_column.append(Uniprot)   

  # for row in rows:
  #         try:
  #           Uniprot_link = row.find_all("td")[4].a['href']
  #           Uniprot_link_column.append(Uniprot_link)
  #         except:
  #           Uniprot_link_column.append("NaN")

  # pdb_column = list()
  # pdb_link_column = list()

  # for table in table_with_pdb:
  #     rows_pdb = table.find_all("tr", valign="top")
  #     pdb_all = str()
  #     pdb_link_all = str()
  #     count = 0
  #     for row in rows_pdb:
  #       try:
  #         pdb = row.find_all("td")[0].text
  #         pdb_all = pdb_all + pdb + "\n"
  #         pdb_link = row.find_all("td")[0].a['href']
  #         pdb_link_all = pdb_link_all + pdb_link + "\n"
  #         count += 1
  #         if count < len(rows_pdb): continue
  #         pdb_column.append(pdb_all)
  #         pdb_link_column.append(pdb_link_all)
  #       except:
  #         pdb_column.append("NA")
  #         pdb_link_column.append("NA")

  # final_table = pd.DataFrame({"Protein Name" : protein_column, "EC" : EC_column, "EC_link" : EC_link_column, "Organism" : Organism_column, 
  #                             "Organism_link" : Organism_link_column, "Genbank" : Genbank_column, "Genbank_link" : Genbank_link_column,
  #                             "Uniprot" : Uniprot_column, "Uniprot_link" : Uniprot_link_column, "PDB" : pdb_column, "PDB_links" : pdb_link_column})

  # final_file = final_table.to_csv("gh2_file_final.cvs", index=False)
