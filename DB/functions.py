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
    db_url = 'mariadb+pymysql://{}:{}@{}:{}/{}?use_unicode=1&charset=utf8'.format(db_user,db_password,db_host,db_port,db_name)
    engine = create_engine(db_url,encoding='utf-8')
    return engine

#Create database schema using sqlalchemy.orm
def createDB(password=None):
    engine = connectDB(password)
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship
    from sqlalchemy import Column, Integer, String, ForeignKeyConstraint, PrimaryKeyConstraint, Text, Table, ForeignKey, UniqueConstraint
    
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

    class CazyFamily(Base):
        __tablename__ = 'CazyFamilies'
        FamilyID = Column(String(10), primary_key=True)
        FamilyClass = Column(String(255))
        ProteinSequences = relationship("ProteinSequence2CazyFamily",
                                back_populates="CazyFam")
        __table_args__ = (
            {'mariadb_engine':'InnoDB'},
        )
    
    class CazyFamilyInfo(Base):
        __tablename__ = 'CazyFamilyInfo'
        ID= Column(Integer, primary_key=True, autoincrement=True)
        FamilyID = Column(String(10))
        Key = Column(String(255))
        Value = Column(String(255))
        __table_args__ = (
            ForeignKeyConstraint(['FamilyID'], ['CazyFamilies.FamilyID']),
            {'mariadb_engine':'InnoDB'},
        )
    
    class ProteinSequence(Base):
        __tablename__ = 'ProteinSequences'
        ProteinID = Column(String(255), primary_key=True)
        Sequence  = Column(Text)
        HashSequence= Column(String(255))
        CazyFamilies = relationship("ProteinSequence2CazyFamily",
                                back_populates="ProteinSeq")
        __table_args__ = (
            {'mariadb_engine':'InnoDB'},
        )

    class ProteinSequence2CazyFamily(Base):
        __tablename__ = 'ProteinSequence2CazyFamily'
        CazyFamilyID = Column(String(10), ForeignKey('CazyFamilies.FamilyID'), primary_key=True)
        ProteinID = Column(String(255), ForeignKey('ProteinSequences.ProteinID'), primary_key=True)
        CazyFam = relationship("CazyFamily",back_populates='ProteinSequences')
        ProteinSeq = relationship("ProteinSequence",back_populates='CazyFamilies')
        __table_args__ = (
            {'mariadb_engine':'InnoDB'},
        )    

    Base.metadata.create_all(engine)

#Drop all tables from DB
def dropDB(password=None):
    engine = connectDB(password)
    from sqlalchemy.ext.automap import automap_base
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Base.metadata.drop_all(engine)

#Drop the tables with info from the CAZy website
def dropDBWebCAZyTables(password=None):
    engine = connectDB(password)
    from sqlalchemy import inspect
    from sqlalchemy.ext.automap import automap_base
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    listTables=['CazyFamilyInfo']
    for tbl in listTables:
        if inspect(engine).has_table(tbl):
            print(f'Dropping table \'{tbl}\'')
            Base.metadata.tables[tbl].drop(engine)
        else:
            print(f'Table \'{tbl}\' does not exists yet in DB.. Cannot delete.')



#Populate Genomes table with data from NCBI genomes
def populateWebCAZyInfo(password=None,updateNCBITaxDB=False,data=None):
    #CAZymes classes
    CAZymeClass={
    'GH':'Glycoside Hydrolases',
    'GT':'GlycosylTransferases',
    'PL':'Polysaccharide Lyases',
    'CE':'Carbohydrate Esterases',
    'AA':'Auxiliary Activities',
    'CBM':'Carbohydrate Binding Modules'
    }
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from sqlalchemy.ext.automap import automap_base
    from ete3 import NCBITaxa
    import re
    import sys
    from unidecode import unidecode


    ncbi = NCBITaxa()
    
    if updateNCBITaxDB:#TODO. This is not working. Check it.
        ncbi.update_taxonomy_database()

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    CazyFamily = Base.classes.CazyFamilies
    CazyFamilyInfo = Base.classes.CazyFamilyInfo

    for family in data:
        match=re.search(r'^(GH|GT|PL|CE|AA|CBM)\d+',family)
        if match:
            print(f'{match.group(1)}\t{CAZymeClass[match.group(1)]}')
            checkFamily=select([CazyFamily]).where(CazyFamily.FamilyID==family)
            resultCheckFamily = session.execute(checkFamily)
            if resultCheckFamily.fetchone():
                print(f'Already saw {family}')
                # continue
            else:
                print(f'Processing CAZy family: {family}')
                session.add(CazyFamily(FamilyID=family, FamilyClass=CAZymeClass[match.group(1)])) 
            for key in data[family]:
                for item in data[family][key]:
                    item=unidecode(item)
                    #print(f'{key}\t{item}')
                    matchEC=re.search(r'\((EC [0-9.-]*)\)',item)
                    checkFamilyInfo=select([CazyFamilyInfo]).where(CazyFamilyInfo.FamilyID==family).where(CazyFamilyInfo.Key==key).where(CazyFamilyInfo.Value==item)
                    resultCheckFamilyInfo = session.execute(checkFamilyInfo)
                    if resultCheckFamilyInfo.fetchone():
                        print(f'Already saw {family}\t{key}\t{item}')
                    else:
                        session.add(CazyFamilyInfo(FamilyID=family, Key=key, Value=item))
                    if matchEC:
                        checkFamilyInfoEC=select([CazyFamilyInfo]).where(CazyFamilyInfo.FamilyID==family).where(CazyFamilyInfo.Key=='Enzyme Code').where(CazyFamilyInfo.Value==matchEC.group(1))
                        resultCheckFamilyInfoEC = session.execute(checkFamilyInfoEC)
                        if resultCheckFamilyInfoEC.fetchone():
                            print(f'Already saw {family}\tEnzyme Code\t{matchEC.group(1)}')
                        else:
                            session.add(CazyFamilyInfo(FamilyID=family, Key='Enzyme Code', Value=matchEC.group(1)))
            #commit the changes
            session.commit()
        else:
            print(f'{family}\tUnknown',file=sys.stderr)

    #commit the changes
    session.commit()
    #close the session
    session.close()
        


def populateGenomes(url,password=None,updateNCBITaxDB=False,typeOrg='euk'):
    #print(f'{typeOrg} {updateNCBITaxDB}')
    engine = connectDB(password)

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from sqlalchemy.ext.automap import automap_base
    from ete3 import NCBITaxa
    import urllib.request
    import ftplib
    import time

    ncbi = NCBITaxa()
    
    if updateNCBITaxDB:#TODO. This is not working. Check it.
        ncbi.update_taxonomy_database()
    
    if typeOrg == 'euk':
        assemblyAccessionIndex=8
    else:
        assemblyAccessionIndex=18

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
                checkGenome=select([Genome]).where(Genome.AssemblyAccession==fields[assemblyAccessionIndex])
                resultCheckGenome = session.execute(checkGenome)
                if resultCheckGenome.fetchone():
                    print(f'Already saw {fields[assemblyAccessionIndex]}')
                    continue
                else:
                    print(f'Checking {fields[assemblyAccessionIndex]}')

                #Commit to the DB every 10 lines of the genome file
                if counter % 10 == 0:
                    session.commit()

                #If there is no taxonomy info for the genome, control the error and continue
                try:
                    lineage = ncbi.get_lineage(int(fields[1]))
                except:
                    errorsLog.write(f'Error: Missing TaxID from NCBI DB: {fields[1]} for assembly: {fields[assemblyAccessionIndex]}\n')
                    continue
                #Only processes genomes with a taxonomy ID in fungi, archaea or bacteria
                targetGroups={4751,2157,2}
                if targetGroups.intersection(set(lineage)):
                    counter+=1
                    print(f'\tAdding Genome Info for Assembly {fields[assemblyAccessionIndex]}')
                    if(lineage[-1] != fields[1]):
                        errorsLog.write(f'Warning: TaxID from genome file: {fields[1]} was translated to: {lineage[-1]} for assembly: {fields[assemblyAccessionIndex]}\n')
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
                    checkGenome=select([Genome]).where(Genome.AssemblyAccession==fields[assemblyAccessionIndex])
                    resultCheckGenome = session.execute(checkGenome)
                    if resultCheckGenome.fetchone() is None:
                        acc,ver=fields[assemblyAccessionIndex].split('.')
                        if len(acc) == 13:
                            FTP_USER = "anonymous"
                            FTP_PASS = ""
                            FTP_HOST = "ftp.ncbi.nlm.nih.gov"
                            ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
                            ftp.cwd(f'genomes/all/{acc[0:3]}/{acc[4:7]}/{acc[7:10]}/{acc[10:13]}/')
                            for asm in ftp.nlst():
                                if asm.startswith(fields[assemblyAccessionIndex]):
                                    url=f'https://ftp.ncbi.nlm.nih.gov/genomes/all/{acc[0:3]}/{acc[4:7]}/{acc[7:10]}/{acc[10:13]}/{asm}'
                                    # print(f'{acc}\t{ver}\t{asm}\t{url}')
                                    #Sometimes the NCBI can update TaxIDs, i.e., translate the taxID. NCBITaxa deals with this, but to avoid foreigkey errors
                                    #it is better to insert into the DB what NCBITaxa retrieved and not the TaxID in the genome file
                                    session.add(Genome(AssemblyAccession=fields[assemblyAccessionIndex], TaxID=lineage[-1], urlBase=url))
                                    ftp.cwd(asm)
                                    for file in ftp.nlst():
                                        if file.endswith(asm + '_genomic.fna.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[assemblyAccessionIndex], FileType='Genome sequence', FileName=file, FileSource='NCBI'))
                                        elif file.endswith(asm + '_protein.faa.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[assemblyAccessionIndex], FileType='Protein sequence', FileName=file, FileSource='NCBI'))	
                                        elif file.endswith(asm + '_genomic.gff.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[assemblyAccessionIndex], FileType='Genome annotation', FileName=file, FileSource='NCBI'))
                                        elif file.endswith(asm + '_translated_cds.faa.gz'):
                                            session.add(GenomeFile(AssemblyAccession=fields[assemblyAccessionIndex], FileType='Protein sequence alter', FileName=file, FileSource='NCBI'))
                                        # print(file)
                            ftp.quit()
                            time.sleep(3)#Wait to avoid being banned by NCBI
                else:
                    print(f'\t{fields[assemblyAccessionIndex]} with TaxID: {fields[1]} is not in the target groups')
    #commit the changes
    session.commit()
    #close the session
    session.close()

# def GenomeInfoStats(password=None):
#     engine = connectDB(password)

#     from sqlalchemy.orm import sessionmaker
#     from sqlalchemy import select
#     from sqlalchemy.sql import text
#     from sqlalchemy.ext.automap import automap_base
#     from ete3 import NCBITaxa
    
#     #create a session
#     Base = automap_base()
#     Base.prepare(engine, reflect=True)
#     Session = sessionmaker(bind=engine)
#     session = Session()
    
#     Genome = Base.classes.Genomes
#     Taxonomy = Base.classes.Taxonomy
#     GenomeFile = Base.classes.GenomeFiles

#     hierachicalQuery = text("""with recursive ancestors as ( 
#         select * from Taxonomy where TaxID in ( 
#             select TaxID from Taxonomy 
#              where RankName='species' and TaxName like 'A%'
#             ) 
#             union 
#             select f.*  from Taxonomy as f, ancestors as a 
#             where  f.TaxID = a.ParentTaxID 
#             )
#             select a.TaxName as labels, b.TaxName as Parent 
#             from ancestors as a, Taxonomy as b 
#             where a.ParentTaxID = b.TaxID""")
#     session.execute(hierachicalQuery).fetchall()


#     session.close()