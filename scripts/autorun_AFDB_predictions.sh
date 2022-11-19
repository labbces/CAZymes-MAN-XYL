#! /usr/bin/bash

for file in /home/dan/CAZymes-MAN-XYL/uniprot_ids_data/uniprot_ids_nov18/*.txt;
do
    family=$(echo $file | cut -d'/' -f 7 |sed 's/ids.txt//g')
    echo $family
    python3 /home/dan/CAZymes-MAN-XYL/alphafold_data/getting_predictions_AFDB.py --input $file --output "${family}/ro"
done