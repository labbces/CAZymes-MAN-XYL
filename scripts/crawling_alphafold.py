# -*- coding: utf-8 -*-

# IMPORTS
import argparse
import os
from time import sleep

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

# Handling arguments by command line
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help="input with list of uniprot IDs", type=str, required=True)
parser.add_argument("-o", "--output", help="path to write output files", type=str, required=True)
args = parser.parse_args()

# Creating directory if already not exists
if not os.path.exists(args.output):
    os.makedirs(args.output)

# Settign options for Chrome
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
navegador = webdriver.Chrome("/usr/local/bin/chromedriver", options=chrome_options)

# Setting couting variables
sucess = 0
fail = 0
fail_list = []

# Acessing AlphaFold with Selenium
with open(args.input) as fhand:
  for line in fhand:
    try:
      line = line.rstrip()
      navegador.get("https://alphafold.ebi.ac.uk/entry/"+line)
      sleep(1)
      pdbfile = navegador.find_element_by_xpath("/html/body/div/app-root/section/app-entry/div[1]/div/app-summary-text/div/div[1]/div[2]/a[1]")
      url = pdbfile.get_attribute('href')
      #os.system('wget %s' % url)
      os.system(f'curl -o {args.output}/{line}.pdb {url}')
      sucess += 1
    except:
      fail += 1
      fail_list.append(line)
      pass

print("Sucess: ", sucess, "Failed: ", fail)
with open(args.output+"/failed.txt", "w") as f:
  for item in fail_list:
    f.write(item+"\n")
print("Failed list saved in: ", args.output+"/failed.txt")
navegador.close()