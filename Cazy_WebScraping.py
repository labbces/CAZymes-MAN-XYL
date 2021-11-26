import sys
from os.path import exists, getmtime
from bs4 import BeautifulSoup
import urllib.request
import time
import pandas as pd


url = "http://www.cazy.org/GH2_structure.html"


if exists('GH2_structure.html'):
  print("Structure CAZY file for family GH2 exists,  checking age", file=sys.stderr)
  mtime=getmtime('GH2_structure.html')
  now=time.time()
  if (now - mtime) > 604800: #Download the file again if the file in disk is older than 7 days (60*60*24*7)
    print("Structure CAZY file for family GH2 is older than 7 days, downloading", file=sys.stderr)
    urllib.request.urlretrieve(url, 'GH2_structure.html')
  else:
    print("Structure CAZY file for family GH2 is younger than 7 days, not downloading and processing as it is", file=sys.stderr)
else:
  print("Structure CAZY file for family GH2 does not exist,  start downloading", file=sys.stderr)
  urllib.request.urlretrieve(url, 'GH2_structure.html')
  print("Download complete", file=sys.stderr)
  
# Using local html to avoid problems with with cazy 
with open("GH2_structure.html") as fhand:
  # Using BeatifulSoup to parse the html
  soup = BeautifulSoup(fhand, "lxml")

  table = soup.find("table", id="pos_onglet")
  print(table.text)
  rows = table.find_all("tr", onmouseover="this.bgColor='#F8FFD5';")
  table_with_pdb = soup.find_all("table", border="0", width='100%')

  protein_column = list()
  EC_column = list()
  EC_link_column = list()
  Organism_column = list()
  Organism_link_column = list()
  Genbank_column = list()
  Genbank_link_column = list()
  Uniprot_column = list()
  Uniprot_link_column = list()

  for row in rows:
        protein_name = row.find("td", id="separateur2").text
        protein_column.append(protein_name)
  for row in rows:
        EC = row.find_all("td")[1].text
        if EC == "":
          EC_column.append(NA)
        else:
          EC_column.append(EC)
        try:
          EC_link = row.find_all("td")[1].a['href']
          EC_link_column.append(EC_link)
        except:
          EC_link_column.append("NaN")
  for row in rows:
        try:
          Organism = row.find_all("td")[2].text.strip()
          Organism_column.append(Organism)
          Organism_link = row.find_all("td")[2].a['href']
          Organism_link_column.append(Organism_link)
        except:
          Organism_column.append("NaN")
          Organism_column_link.append("NaN")

  for row in rows:
          Genbank = row.find_all("td")[3].get_text(strip=True)
          Genbank_column.append(Genbank)

  for row in rows:
          try:
            Genbank_link = row.find_all("td")[3].a['href']
            Genbank_link_column.append(Genbank_link)
          except:
            Genbank_link_column.append("NaN")
            
  for row in rows:
          Uniprot = row.find_all("td")[4].get_text(strip=True)
          Uniprot_column.append(Uniprot)   

  for row in rows:
          try:
            Uniprot_link = row.find_all("td")[4].a['href']
            Uniprot_link_column.append(Uniprot_link)
          except:
            Uniprot_link_column.append("NaN")

  pdb_column = list()
  pdb_link_column = list()

  for table in table_with_pdb:
      rows_pdb = table.find_all("tr", valign="top")
      pdb_all = str()
      pdb_link_all = str()
      count = 0
      for row in rows_pdb:
        try:
          pdb = row.find_all("td")[0].text
          pdb_all = pdb_all + pdb + "\n"
          pdb_link = row.find_all("td")[0].a['href']
          pdb_link_all = pdb_link_all + pdb_link + "\n"
          count += 1
          if count < len(rows_pdb): continue
          pdb_column.append(pdb_all)
          pdb_link_column.append(pdb_link_all)
        except:
          pdb_column.append("NA")
          pdb_link_column.append("NA")

  final_table = pd.DataFrame({"Protein Name" : protein_column, "EC" : EC_column, "EC_link" : EC_link_column, "Organism" : Organism_column, 
                              "Organism_link" : Organism_link_column, "Genbank" : Genbank_column, "Genbank_link" : Genbank_link_column,
                              "Uniprot" : Uniprot_column, "Uniprot_link" : Uniprot_link_column, "PDB" : pdb_column, "PDB_links" : pdb_link_column})

  final_file = final_table.to_csv("gh2_file_final.cvs", index=False)
