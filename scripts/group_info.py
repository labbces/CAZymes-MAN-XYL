# Imports
import re
import argparse

# Getting proteins sequences file
parser = argparse.ArgumentParser()
parser.add_argument(
    "ProteinsFile", help="file that contains protein sequences per family")
args = parser.parse_args()

ptn_file = args.ProteinsFile

group_dict = {}
status_dict = {}
# Getting ID line
with open(ptn_file, 'r') as ptn_file:
    for line in ptn_file:
        if ">" in line:
            # Gettin sequence Group and Status
            id4search = re.search(
                r'>(.*) Status:\[(.*)\];(.*):\[(.*)\];CazyFamily:\[(.*)\];taxID:\[(.*)\];name:\[(.*)\];species:\[(.*)\];Group:\[(.*)\]', line)
            Group = str(id4search.group(9))
            status = str(id4search.group(2))
# Counting sequences by group
            if Group not in group_dict.keys():
                group_dict[Group] = 1
            else:
                group_dict[Group] += 1
# Counting sequences by status and group
            if status not in status_dict:
                status_dict[status] = {}
                if Group not in status_dict[status].keys():
                    status_dict[status][Group] = 1
                else:
                    status_dict[status][Group] += 1
            else:
                if Group not in status_dict[status].keys():
                    status_dict[status][Group] = 1
                else:
                    status_dict[status][Group] += 1
fam = str(id4search.group(5))
print('\n', '=== ', fam, " ===")
print(group_dict)
print(status_dict, '\n')
