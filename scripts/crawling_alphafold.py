# -*- coding: utf-8 -*-

# IMPORTS
import os
from time import sleep
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

# Settign options for Chrome
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
navegador = webdriver.Chrome("/usr/local/bin/chromedriver", options=chrome_options)


# Setting couting variables
sucess = 0
failed = 0

# Acessing AlphaFold with Selenium
with open("uniprot-compressed_true_download_true_format_list-2022.11.12-22.29.34.11.list") as fhand:
  for line in fhand:
    try:
      line = line.rstrip()
      navegador.get("https://alphafold.ebi.ac.uk/entry/"+line)
      sleep(1)
      pdbfile = navegador.find_element_by_xpath("/html/body/div/app-root/section/app-entry/div[1]/div/app-summary-text/div/div[1]/div[2]/a[1]")
      url = pdbfile.get_attribute('href')
      os.system('wget %s' % url)
      sucess += 1
    except:
      failed += 1
      pass

print("Sucess: ", sucess, "Failed: ", failed)


navegador.close()



