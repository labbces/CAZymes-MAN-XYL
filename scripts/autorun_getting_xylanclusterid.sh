#! /usr/bin/bash
path="/home/dan/CAZymes-MAN-XYL/uniprot_ids_data/uniprot_xylan_clusters_ids_nov19"
for file in /home/dan/CAZymes-MAN-XYL/uniprot_ids_data/xylan_cluster_nov18/*.txt;
do
    echo $file
    python3 /home/dan/CAZymes-MAN-XYL/grupos/getting_xylan_clusters.py -cn $file -o $path
done