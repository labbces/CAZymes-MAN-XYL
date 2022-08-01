# Annotation of OTUs using NCBI Taxonomy
def AnnotationOTUs(treefile):
    from ete3 import NCBITaxa; ncbi = NCBITaxa()
    for node in treefile.traverse("postorder"):
        try:
            if node.is_leaf():
                taxid = node.name.split("__")[7]
                lineage = ncbi.get_lineage(int(taxid))
                names = ncbi.get_taxid_translator(lineage)
                OTUs = [names[id] for id in lineage] # se usa o lineage ao inves do rank_names keys para se manter a ordem das OTUs
                ranks = ncbi.get_rank(lineage)
                life_structure = {}
                for i in range(len(OTUs)):
                    life_structure[ranks[lineage[i]]] = OTUs[i]                    
                for feature,value in life_structure.items():
                    if feature != "no rank":
                        node.add_feature(feature,value)
        except:
            print("Error:", node)
    return treefile

# Annotation of CD-HTI Hierarchical Clustering Data
def AnnotationClusterCDHit(treefile, clusters_data, metadata_data):
    import pandas as pd
    clusters = pd.read_csv(clusters_data, sep="\t", index_col=0)    
    metadata = pd.read_csv(metadata_data, sep="\t")
    not_studied_clusters = clusters.copy()
    unique = metadata['cluster_id'].unique()
    not_studied_clusters = not_studied_clusters[not_studied_clusters['Cluster'].map(lambda x: x in unique)] 
    nt_dict = not_studied_clusters.to_dict() # convert cluster data into dictionary
    
    for node in treefile.traverse("postorder"): 
        if node.is_leaf():
            try:
                id = node.name.split("_")[0].strip()
                if id in nt_dict['Cluster'].keys():      
                    node.add_feature("cdhit_cluster", nt_dict['Cluster'][id])
            except:
                print("Parsing Error In:", node)
    return treefile

# Annotation of ECs Data
def AnnotationsECs(treefile, ECs_data):
    import pandas as pd
    ECs = pd.read_csv(ECs_data, sep=";", index_col=1)
    ECs_dict = ECs.to_dict()
    ECs_dict.keys()

    for node in treefile.traverse("postorder"):     
        if node.is_leaf():
            try:
                id = node.name.split("_")[0].strip()
                if id in ECs_dict['ECs'].keys():   
                    node.add_feature("EC", ECs_dict['ECs'][id])  # add ECs to each leaf
            except:
                print("Parsing Error In:", node)
    return treefile

# Annotations of TreeGubbins Data
def AnnotationTreeGubbins(treefile, treegubbins_data):
    treegubbins = dict()
    with open(treegubbins_data, "r") as f:
        for line in f:
            if not line.startswith("Taxon"):
                    cluster = line.split(",")[1]
                    id = line.split(",")[0].split("Status")[0].strip()[:-1] # remove the last character, ugly but works
                    if cluster not in treegubbins.keys():
                        treegubbins[id] = cluster

    for node in treefile.traverse("postorder"):
        if node.is_leaf():
            try:
                id = node.name.split("_")[0].strip()
                if id in treegubbins.keys():   
                    node.add_feature("cluster_tg", treegubbins[id])  # add cluster id from tree gubbins to each leaf
            except:
                print("Parsing Error In:", node)
    return treefile