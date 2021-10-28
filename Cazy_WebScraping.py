from bs4 import BeautifulSoup
import requests
import time
import pandas as pd

# Using BeatifulSoup to parse
'''html_text = requests.get('http://www.cazy.org/GH1_structure.html').text
soup = BeautifulSoup(html_text, 'lxml')
table = soup.find('table', class_="listing")
print(table)'''

# Using pandas_html to parse from web
#df = pd.read_html('http://www.cazy.org/GH1_structure.html',skiprows=1)

#using local html to avoid problems with with cazy 
file_path = 'cazy.html'
with open(file_path, 'r') as f:
    df = pd.read_html(f.read(), skiprows=1)

structure_table = df[1]
structure_table.to_csv('gh1_structure.cvs',index='false')
new_table = structure_table.iloc[:-1,:]
new_table.to_csv("gh1_structure.csv")
