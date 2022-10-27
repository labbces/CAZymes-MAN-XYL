# %%
import pandas as pd
from os import mkdir

# %%
pdb = pd.read_csv('pdb.csv', sep=';', index_col=1)
families = ['GH31','GH115','CE6','GH116','GH67','GH97','GH54','GH95','CE16','GH62','GH30','GH39','GH8','CE5','CE4','CE1','GH11','GH51','GH10','GH43']
newpdb = pdb[pdb['family'].isin(families)]
newpdb.to_csv('pdb2.csv', sep=';') # Save the data with only the target families
checking = newpdb.value_counts('family')
# for value in newpdb.family.unique():
#     print(value, newpdb[newpdb.family == value].value_counts('family').max())
notinlist = list(set(families) - set(newpdb.family.unique()))

# %%

uniques = {col: len(newpdb[col].unique()) for col in newpdb}

sort_orders = sorted(uniques.items(), key=lambda x: x[1])

for i in sort_orders:
	print(i[0], i[1])

# %%
pdb_with_count = newpdb.value_counts(ascending=True).reset_index(name='count').sort_values(by=['family','count'], ascending=[True,False])
#pdb_with_count.to_csv('pdb3.csv', sep=';', index=False)
for family in pdb_with_count.family.unique():
    try:
        os.mkdir(family)
    except:
        print(f'Directory {family} already exists')
    family_file = pdb_with_count[pdb_with_count.family == family]
    family_file.to_csv(family + '_pdb_all' '.csv', sep=';', index=False)
    with open(f'{family}/{family}_pdb_ids2.txt', 'w') as f:
        ids = family_file['PDBID'].to_string(header=False, index=False)
        for id in ids.split():
            f.write(id + ",")



