
#create connection object to postgresql database using sqlalchemy
def connectDB():
    import sqlalchemy
    from sqlalchemy import create_engine
    #connect to postgresql database
    db_user = 'cazymes'
    db_password = 'cazymes'
    db_name = 'cazymes'
    db_host = 'localhost'
    db_port =  5432
    db_url = 'postgresql://{}:{}@{}:{}/{}'.format(db_user,db_password,db_host,db_port,db_name)
    engine = create_engine(db_url)
    return engine

#Create database schema using sqlalchemy.orm
def createDB():
    engine = connectDB()
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, ForeignKeyConstraint, PrimaryKeyConstraint
    
    #create a table
    Base = declarative_base()
    class Taxonomy(Base):
        __tablename__ = 'Taxonomy'
        #id = Column(Integer, primary_key=True)
        TaxID = Column(Integer)
        TaxIDRank = Column(Integer)
        RankName = Column(String)
        TaxName = Column(String)
        __table_args__ = (
            PrimaryKeyConstraint(TaxID, TaxIDRank),{},
        #    ForeignKeyConstraint(['TaxID'], ['Taxonomy.TaxID']),
            )

    class Genome(Base):
        __tablename__ = 'Genomes'
        #__table_args__ = (
        #    ForeignKeyConstraint(['TaxID'], ['Taxonomy.TaxID']),
        #    )
        AssemblyAccession = Column(String, primary_key=True)
        TaxID = Column(Integer)
        urlBase = Column(String)
    
        
    Base.metadata.create_all(engine)

#Populate Genomes table with data from NCBI genomes
def populateGenomes(url):
    engine = connectDB()
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from sqlalchemy.ext.automap import automap_base
    from ete3 import NCBITaxa
    import urllib.request
    import ftplib
    import time

    ncbi = NCBITaxa()
    counter=0
    #create a session
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    Genome = Base.classes.Genomes
    Taxonomy = Base.classes.Taxonomy

    #get the data from the NCBI genomes and add to tables
    for line in urllib.request.urlopen(url):
        line = line.decode('utf-8')
        line = line.rstrip()
        fields = line.split('\t')
        if line.startswith('#'):
            continue
        else:
            if counter % 10 == 0:
                #commit the changes
                session.commit()
                time.sleep(4)
            lineage = ncbi.get_lineage(int(fields[1]))
            #Only processes genomes with a taxonomy ID in fungi, archaea or bacteria
            targetGroups={4751,2157,2}
            if targetGroups.intersection(set(lineage)):
                counter+=1
                for i in ncbi.get_lineage(fields[1]):
                    if i == 1:
                        continue    
                    else:  
                        checkPair=select([Taxonomy]).where(Taxonomy.TaxID==fields[1]).where(Taxonomy.TaxIDRank==i)
                        resultCheckPair = session.execute(checkPair)
                        if resultCheckPair.fetchone() is None:
                            taxid2name = ncbi.get_taxid_translator([i])
                            rank = ncbi.get_rank([i])
                            #print(f'{taxid2name[i]}\t{rank[i]}')
                            session.add(Taxonomy(TaxID=fields[1], TaxIDRank=i, RankName=rank[i], TaxName=taxid2name[i]))
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
                                print(f'{acc}\t{ver}\t{asm}\t{url}')
                                session.add(Genome(AssemblyAccession=fields[8], TaxID=fields[1], urlBase=url))
                        ftp.quit()
    #commit the changes
    session.commit()
    #close the session
    session.close()

createDB()
populateGenomes('https://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/eukaryotes.txt')