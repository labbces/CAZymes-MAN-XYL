#Functions for XYLMAN project


#create connection object to mysql/mariadb database using sqlalchemy
def connectDB(password=None):
    import sqlalchemy
    from sqlalchemy import create_engine
    #connect to postgresql database
    db_user = 'xylman'
    db_password = password
    db_name = 'xylman'
    db_host = '200.144.245.42'
    db_port =  3306
    db_url = 'mariadb+pymysql://{}:{}@{}:{}/{}'.format(db_user,db_password,db_host,db_port,db_name)
    engine = create_engine(db_url)
    return engine

    #Create database schema using sqlalchemy.orm
def createDB(password=None):
    engine = connectDB(password)
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, ForeignKeyConstraint, PrimaryKeyConstraint
    
    #create a table
    Base = declarative_base()
    class Taxonomy(Base):
        __tablename__ = 'Taxonomy'
        #id = Column(Integer, primary_key=True)
        ParentTaxID = Column(Integer, nullable=True)
        TaxID = Column(Integer, primary_key=True, autoincrement=False)
        #TaxIDRank = Column(Integer)
        RankName = Column(String(255))
        TaxName = Column(String(255))
        __table_args__ = (
            PrimaryKeyConstraint(TaxID),
            {'mariadb_engine':'InnoDB'},
        )

    class Genome(Base):
        __tablename__ = 'Genomes'
        __table_args__ = (
            ForeignKeyConstraint(['TaxID'], ['Taxonomy.TaxID']),
            {'mariadb_engine':'InnoDB'},
                    )
        AssemblyAccession = Column(String(100), primary_key=True)
        TaxID = Column(Integer)
        urlBase = Column(String(255))

    class GenomeFile(Base):
        __tablename__ = 'GenomeFiles'
        AssemblyAccession = Column(String(100))
        FileSource = Column(String(255))
        FileType = Column(String(100))
        FileName = Column(String(255))
        __table_args__ = (
            ForeignKeyConstraint(['AssemblyAccession'], ['Genomes.AssemblyAccession']),
            PrimaryKeyConstraint(FileType, AssemblyAccession),
            {'mariadb_engine':'InnoDB'},
                    )
    
    # class Genome2Taxonomy(Base):
    #     __tablename__ = 'Genome2Taxonomy'
    #     AssemblyAccession = Column(String(100))
    #     TaxID = Column(Integer)
    #     __table_args__ = (
    #         PrimaryKeyConstraint(TaxID, AssemblyAccession),
    #         ForeignKeyConstraint(['TaxID'], ['Taxonomy.TaxID']),
    #         ForeignKeyConstraint(['AssemblyAccession'], ['Genomes.AssemblyAccession']),
    #     #    {'mariadb_engine':'InnoDB'},
    #         )

    Base.metadata.create_all(engine)

#Drop all tables from DB
def dropDB(password=None):
    engine = connectDB(password)
    from sqlalchemy.ext.automap import automap_base
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Base.metadata.drop_all(engine)


#Populate Genomes table with data from NCBI genomes
def populateGenomes(url,password=None,updateNCBITaxDB=False):
    
    engine = connectDB(password)

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from sqlalchemy.ext.automap import automap_base
    from ete3 import NCBITaxa
    import urllib.request
    import ftplib
    import time

    ncbi = NCBITaxa()
    
    if updateNCBITaxDB:
        ncbi.update_taxonomy_database()
    
    counter=0
    #create a session
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    Genome = Base.classes.Genomes
    Taxonomy = Base.classes.Taxonomy
    GenomeFile = Base.classes.GenomeFiles

    #get the data from the NCBI genomes and add to tables
    urllib.request.urlretrieve(url,'genomes.txt')
    with open('genomes.txt', mode='r', encoding="utf8") as genomeFile, open('populateErrors.log','w') as errorsLog:
        for line in genomeFile:
            #line = line.decode('utf-8')
            line = line.rstrip()
            fields = line.split('\t')
            if line.startswith('#'):
                continue
            else:
                #If the genome info was already inserted skip it
                checkGenome=select([Genome]).where(Genome.AssemblyAccession==fields[8])
                resultCheckGenome = session.execute(checkGenome)
                if resultCheckGenome.fetchone():
                    print(f'Already saw {fields[8]}')
                    continue
                else:
                    print(f'Checking {fields[8]}')

                #Commit to the DB every 10 lines of the genome file
                if counter % 10 == 0:
                    session.commit()

                #If there is no taxonomy info for the genome, control the error and continue
                try:
                    lineage = ncbi.get_lineage(int(fields[1]))
                except:
                    errorsLog.write(f'Error: Missing TaxID from NCBI DB: {fields[1]} for assembly: {fields[8]}\n')
                    continue
                #Only processes genomes with a taxonomy ID in fungi, archaea or bacteria
                targetGroups={4751,2157,2}
                if targetGroups.intersection(set(lineage)):
                    counter+=1
                    # print(lineage)
                    if(lineage[-1] != fields[1]):
                        errorsLog.write(f'Warning: TaxID from genome file: {fields[1]} was translated to: {lineage[-1]} for assembly: {fields[8]}\n')
                    for index, i in enumerate(lineage):
                        checkTaxID=select([Taxonomy]).where(Taxonomy.TaxID==i)
                        resultCheckTaxID = session.execute(checkTaxID)
                        if i == 1:
                            #Check whether the taxonomy info is already in the DB. If not, add it
                            if resultCheckTaxID.fetchone() is None:
                                taxid2name = ncbi.get_taxid_translator([i])
                                rank = ncbi.get_rank([i])
                                # print(f'{index}\t{i}\t{lineage[index-1]}\t{lineage[index]}\t{taxid2name[i]}\t{rank[i]}')
                                #The root node is added to the DB, as it does not have parent and NA will be inserted for ParentTaxID
                                session.add(Taxonomy(TaxID=i, RankName=rank[i], TaxName=taxid2name[i])) 
                        else:
                            #Check whether the taxonomy info is already in the DB. If not, add it
                            if resultCheckTaxID.fetchone() is None:
                                taxid2name = ncbi.get_taxid_translator([i])
                                rank = ncbi.get_rank([i])
                                # print(f'{index}\t{i}\t{lineage[index-1]}\t{lineage[index]}\t{taxid2name[i]}\t{rank[i]}')
                                session.add(Taxonomy(ParentTaxID=lineage[index-1], TaxID=i, RankName=rank[i], TaxName=taxid2name[i]))
                    checkGenome=select([Genome]).where(Genome.AssemblyAccession==fields[8])
                    resultCheckGenome = session.execute(checkGenome)
                    if resultCheckGenome.fetchone() is None:
                        acc,ver=fields[8].split('.')
                        if len(acc) == 13:
                            FTP_USER = "anonymous"
                            FTP_PASS = ""
                            FTP_HOST = "ftp.ncbi.nlm.nih.gov"
                            ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
                            ftp.cwd(f'genomes/all/{acc[0:3]}/{acc[4:7]}/{acc[7:10]}/{acc[10:13]}/')
                            for asm in ftp.nlst():
                                if asm.startswith(fields[8]):
                                    url=f'https://ftp.ncbi.nlm.nih.gov/genomes/all/{acc[0:3]}/{acc[4:7]}/{acc[7:10]}/{acc[10:13]}/{asm}'
                                    # print(f'{acc}\t{ver}\t{asm}\t{url}')
                                    #Sometimes the NCBI can update TaxIDs, i.e., translate the taxID. NCBITaxa deals with this, but to avoid foreigkey errors
                                    #it is better to insert into the DB what NCBITaxa retrieved and not the TaxID in the genome file
                                    session.add(Genome(AssemblyAccession=fields[8], TaxID=lineage[-1], urlBase=url))
                                    ftp.cwd(asm)
                                    for file in ftp.nlst():
                                        if file.endswith(asm + '_genomic.fna.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[8], FileType='Genome sequence', FileName=file, FileSource='NCBI'))
                                        elif file.endswith(asm + '_protein.faa.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[8], FileType='Protein sequence', FileName=file, FileSource='NCBI'))	
                                        elif file.endswith(asm + '_genomic.gff.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[8], FileType='Genome annotation', FileName=file, FileSource='NCBI'))
                                        elif file.endswith(asm + '_translated_cds.faa.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[8], FileType='Protein sequence alter', FileName=file, FileSource='NCBI'))
                                        # print(file)
                            ftp.quit()
                            time.sleep(3)#Wait to avoid being banned by NCBI
                else:
                    print(f'\t{fields[8]} with TaxID: {fields[1]} is not in the target groups')
    #commit the changes
    session.commit()
    #close the session
    session.close()
