#Functions for XYLMAN project

#create connection object to mysql/mariadb database using sqlalchemy
#from hashlib import md5
#from re import I
from sqlalchemy.sql.sqltypes import Enum, String


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
    from sqlalchemy import Column, Integer, String, ForeignKeyConstraint, PrimaryKeyConstraint, Text, Date, ForeignKey, UniqueConstraint
    
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
        ID=Column(Integer, primary_key=True, autoincrement=True)
        AssemblyAccession = Column(String(100))
        FileSource = Column(String(255))
        FileType = Column(String(100))
        FileName = Column(String(255))
        __table_args__ = (
            ForeignKeyConstraint(['AssemblyAccession'], ['Genomes.AssemblyAccession']),
            UniqueConstraint(FileType, AssemblyAccession),
            {'mariadb_engine':'InnoDB'},
                    )
    
    class GenomeFileDownloaded(Base):
        __tablename__ = 'GenomeFileDownloaded'
        ID=Column(Integer, primary_key=True, autoincrement=True)
        GenomeFileID = Column(Integer)
        Action=Column(String(255))
        ActionDate = Column(Date)
        __table_args__ = (
            ForeignKeyConstraint(['GenomeFileID'], ['GenomeFiles.ID']),
            UniqueConstraint(GenomeFileID, Action, ActionDate),
            {'mariadb_engine':'InnoDB'},
        )

    class Protein2GenomeFile(Base):
        __tablename__ = 'Proteins2GenomeFile'
        ID=Column(Integer, primary_key=True, autoincrement=True)
        GenomeFileID = Column(Integer, index=True)
        ProteinID=Column(String(255))
        __table_args__ = (
            ForeignKeyConstraint(['GenomeFileID'], ['GenomeFiles.ID']),
            ForeignKeyConstraint(['ProteinID'], ['ProteinSequences.ProteinID']),
            UniqueConstraint(GenomeFileID,ProteinID),
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
        Value = Column(Text)
        __table_args__ = (
            ForeignKeyConstraint(['FamilyID'], ['CazyFamilies.FamilyID']),
            {'mariadb_engine':'InnoDB'},
        )
    
    class ProteinSequence(Base):
        __tablename__ = 'ProteinSequences'
        ProteinID = Column(String(255), primary_key=True)
        Database=Column(String(50))
        Sequence  = Column(Text)
        HashSequence= Column(String(255))
        CazyFamilies = relationship("ProteinSequence2CazyFamily",
                               back_populates="ProteinSeq")
        __table_args__ = (
            {'mariadb_engine':'InnoDB'},
        )

    class ProteinSequence2CazyFamily(Base):
        __tablename__ = 'ProteinSequence2CazyFamily'
        ID=Column(Integer, primary_key=True, autoincrement=True)
        CazyFamilyID = Column(String(10), ForeignKey('CazyFamilies.FamilyID'))
        ProteinID = Column(String(255), ForeignKey('ProteinSequences.ProteinID'))
        CazyFam = relationship("CazyFamily",back_populates='ProteinSequences')
        ProteinSeq = relationship("ProteinSequence",back_populates='CazyFamilies')
        __table_args__ = (
            UniqueConstraint(CazyFamilyID,ProteinID),
            {'mariadb_engine':'InnoDB'},
        )    
    
    class StudiedCAZymes(Base):
        __tablename__ = 'StudiedCAZymes'
        StudiedCAZymesID=Column(Integer, primary_key=True, autoincrement=True)
        TaxID = Column(Integer)
        TaxNameAsIs=Column(String(255))
        Name=Column(String(255))
        Type=Column(Enum('characterized','structure')) #This could be either characterized or structure
        FamilyID = Column(String(10))
        subFamily = Column(Integer, nullable=True)
        __table_args__ = (
            ForeignKeyConstraint(['FamilyID'], ['CazyFamilies.FamilyID']),
            ForeignKeyConstraint(['TaxID'], ['Taxonomy.TaxID']),
            {'mariadb_engine':'InnoDB'},
        )
    
    class StudiedCAZymesPDB(Base):
        __tablename__ = 'StudiedCAZymesPDB'
        ID=Column(Integer, primary_key=True, autoincrement=True)
        StudiedCAZymesID=Column(Integer)
        PDBID = Column(String(10))
        PDBChain=Column(String(20))
        __table_args__ = (
            ForeignKeyConstraint(['StudiedCAZymesID'], ['StudiedCAZymes.StudiedCAZymesID']),
            {'mariadb_engine':'InnoDB'},
        )
    
    class StudiedCAZymesEC(Base):
        __tablename__ = 'StudiedCAZymesEC'
        ID=Column(Integer, primary_key=True, autoincrement=True)
        StudiedCAZymesID=Column(Integer)
        EC = Column(String(20))
        __table_args__ = (
            ForeignKeyConstraint(['StudiedCAZymesID'], ['StudiedCAZymes.StudiedCAZymesID']),
            {'mariadb_engine':'InnoDB'},
        )

    class StudiedCAZymesProteins(Base):
        __tablename__ = 'StudiedCAZymesProteins'
        ID=Column(Integer, primary_key=True, autoincrement=True)
        StudiedCAZymesID=Column(Integer)
        ProteinID = Column(String(255))
        __table_args__ = (
            ForeignKeyConstraint(['StudiedCAZymesID'], ['StudiedCAZymes.StudiedCAZymesID']),
            ForeignKeyConstraint(['ProteinID'], ['ProteinSequences.ProteinID']),
            {'mariadb_engine':'InnoDB'},
        )
    
    class ProteinReference(Base):
        __tablename__ = 'ProteinReferences'
        ProteinReferenceID=Column(Integer, primary_key=True, autoincrement=True)
        StudiedCAZymesID=Column(Integer)
        Reference=Column(String(255))
        Source = Column(String(255))
        __table_args__ = (
            ForeignKeyConstraint(['StudiedCAZymesID'], ['StudiedCAZymes.StudiedCAZymesID']),
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
    listTables=['CazyFamilyInfo','StudiedCAZymesProteins','StudiedCAZymesPDB','StudiedCAZymes']
    for tbl in listTables:
        if inspect(engine).has_table(tbl):
            print(f'Dropping table \'{tbl}\'')
            Base.metadata.tables[tbl].drop(engine)
        else:
            print(f'Table \'{tbl}\' does not exists yet in DB.. Cannot delete.')

def computeMD5Sumfile(pathFile=None):
    import hashlib
    with open(pathFile, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)
    return file_hash.hexdigest()

def getMD5sumFromFile(md5PathFile=None, target=None):
    with open(md5PathFile, "r") as fmd5:
        for line in fmd5:
            line = line.rstrip()
            fields = line.split()
            if fields[1].replace('./','') == target:
                md5sum = fields[0]
                break
    return md5sum

#Get all proteins in fasta format for a given family
def getProteinsFasta(familyID=None, password=None):
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select, func
    from sqlalchemy.ext.automap import automap_base
    import datetime
    import sys
    from Bio import SeqIO
    from Bio.Seq import Seq
    from Bio.SeqRecord import SeqRecord

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    dateToday=datetime.date.today().strftime("%d%m%Y")
    fileOutPredictedCazymeProteins=f'{familyID}_PredictedCAZymeProteins_{dateToday}.fasta'
    fileOutCharacterizedCazymeProteins=f'{familyID}_CharacterizedCAZymeProteins_{dateToday}.fasta'
    fileOutStructureCazymeProteins=f'{familyID}_StructureCAZymeProteins_{dateToday}.fasta'

    GenomeFiles = Base.classes.GenomeFiles
    Genomes = Base.classes.Genomes
    ProteinSequence = Base.classes.ProteinSequences 
    Proteins2GenomeFile = Base.classes.Proteins2GenomeFile
    ProteinSequence2CazyFamily = Base.classes.ProteinSequence2CazyFamily
    StudiedCAZyme = Base.classes.StudiedCAZymes
    StudiedCAZymeProtein = Base.classes.StudiedCAZymesProteins

    getProteinSequencesForFamily=select([ProteinSequence.ProteinID,ProteinSequence.Sequence,GenomeFiles.AssemblyAccession,ProteinSequence2CazyFamily.CazyFamilyID,Genomes.TaxID])
    getProteinSequencesForFamily=getProteinSequencesForFamily.join(Proteins2GenomeFile,Proteins2GenomeFile.ProteinID==ProteinSequence.ProteinID)
    getProteinSequencesForFamily=getProteinSequencesForFamily.join(GenomeFiles,GenomeFiles.ID==Proteins2GenomeFile.GenomeFileID)
    getProteinSequencesForFamily=getProteinSequencesForFamily.join(ProteinSequence2CazyFamily,ProteinSequence2CazyFamily.ProteinID==ProteinSequence.ProteinID)
    getProteinSequencesForFamily=getProteinSequencesForFamily.join(Genomes,Genomes.AssemblyAccession==GenomeFiles.AssemblyAccession)
    getProteinSequencesForFamily=getProteinSequencesForFamily.where(ProteinSequence2CazyFamily.CazyFamilyID==familyID)

    getCountProteinSequencesForFamily=select([func.count(ProteinSequence.ProteinID)])
    getCountProteinSequencesForFamily=getCountProteinSequencesForFamily.join(Proteins2GenomeFile,Proteins2GenomeFile.ProteinID==ProteinSequence.ProteinID)
    getCountProteinSequencesForFamily=getCountProteinSequencesForFamily.join(GenomeFiles,GenomeFiles.ID==Proteins2GenomeFile.GenomeFileID)
    getCountProteinSequencesForFamily=getCountProteinSequencesForFamily.join(ProteinSequence2CazyFamily,ProteinSequence2CazyFamily.ProteinID==ProteinSequence.ProteinID)
    getCountProteinSequencesForFamily=getCountProteinSequencesForFamily.join(Genomes,Genomes.AssemblyAccession==GenomeFiles.AssemblyAccession)
    getCountProteinSequencesForFamily=getCountProteinSequencesForFamily.where(ProteinSequence2CazyFamily.CazyFamilyID==familyID)

    resultsGetCountProteinSequencesForFamily=session.execute(getCountProteinSequencesForFamily)
    numberProteinSequencesForFamily=resultsGetCountProteinSequencesForFamily.fetchone()[0]

    getCharacterizedProteinsForFamily=select([ProteinSequence.ProteinID,ProteinSequence.Sequence,ProteinSequence.Database,StudiedCAZyme.Type,StudiedCAZyme.FamilyID,StudiedCAZyme.TaxID])
    getCharacterizedProteinsForFamily=getCharacterizedProteinsForFamily.join(StudiedCAZymeProtein,StudiedCAZymeProtein.ProteinID==ProteinSequence.ProteinID)
    getCharacterizedProteinsForFamily=getCharacterizedProteinsForFamily.join(StudiedCAZyme,StudiedCAZyme.StudiedCAZymesID==StudiedCAZymeProtein.StudiedCAZymesID)
    getCharacterizedProteinsForFamily=getCharacterizedProteinsForFamily.where(StudiedCAZyme.Type=='characterized')
    getCharacterizedProteinsForFamily=getCharacterizedProteinsForFamily.where(StudiedCAZyme.FamilyID==familyID)

    getCountCharacterizedProteinsForFamily=select([func.count(ProteinSequence.ProteinID)])
    getCountCharacterizedProteinsForFamily=getCountCharacterizedProteinsForFamily.join(StudiedCAZymeProtein,StudiedCAZymeProtein.ProteinID==ProteinSequence.ProteinID)
    getCountCharacterizedProteinsForFamily=getCountCharacterizedProteinsForFamily.join(StudiedCAZyme,StudiedCAZyme.StudiedCAZymesID==StudiedCAZymeProtein.StudiedCAZymesID)
    getCountCharacterizedProteinsForFamily=getCountCharacterizedProteinsForFamily.where(StudiedCAZyme.Type=='characterized')
    getCountCharacterizedProteinsForFamily=getCountCharacterizedProteinsForFamily.where(StudiedCAZyme.FamilyID==familyID)

    resultsGetCountCharacterizedProteinsForFamily=session.execute(getCountCharacterizedProteinsForFamily)
    numberCharacterizedSequencesForFamily=resultsGetCountCharacterizedProteinsForFamily.fetchone()[0]

    getStructureProteinsForFamily=select([ProteinSequence.ProteinID,ProteinSequence.Sequence,ProteinSequence.Database,StudiedCAZyme.Type,StudiedCAZyme.FamilyID,StudiedCAZyme.TaxID])
    getStructureProteinsForFamily=getStructureProteinsForFamily.join(StudiedCAZymeProtein,StudiedCAZymeProtein.ProteinID==ProteinSequence.ProteinID)
    getStructureProteinsForFamily=getStructureProteinsForFamily.join(StudiedCAZyme,StudiedCAZyme.StudiedCAZymesID==StudiedCAZymeProtein.StudiedCAZymesID)
    getStructureProteinsForFamily=getStructureProteinsForFamily.where(StudiedCAZyme.Type=='structure')
    getStructureProteinsForFamily=getStructureProteinsForFamily.where(StudiedCAZyme.FamilyID==familyID)

    getCountStructureProteinsForFamily=select([func.count(ProteinSequence.ProteinID)])
    getCountStructureProteinsForFamily=getCountStructureProteinsForFamily.join(StudiedCAZymeProtein,StudiedCAZymeProtein.ProteinID==ProteinSequence.ProteinID)
    getCountStructureProteinsForFamily=getCountStructureProteinsForFamily.join(StudiedCAZyme,StudiedCAZyme.StudiedCAZymesID==StudiedCAZymeProtein.StudiedCAZymesID)
    getCountStructureProteinsForFamily=getCountStructureProteinsForFamily.where(StudiedCAZyme.Type=='structure')
    getCountStructureProteinsForFamily=getCountStructureProteinsForFamily.where(StudiedCAZyme.FamilyID==familyID)

    resultsGetCountStructureProteinsForFamily=session.execute(getCountStructureProteinsForFamily)
    numberStructureSequencesForFamily=resultsGetCountStructureProteinsForFamily.fetchone()[0]

    resultsGetProteinSequencesForFamily=session.execute(getProteinSequencesForFamily)
    rows=resultsGetProteinSequencesForFamily.fetchall()
    print(f'There are {numberProteinSequencesForFamily} predicted sequences for family {familyID} in DB, {len(rows)} were retrieved from the MySQL db.')
    if rows:
        with open(fileOutPredictedCazymeProteins, "w") as f:
            for row in rows:
                lineage=getTaxInfo(taxID=row[4],password=password)
                proteinRecord = SeqRecord(Seq(row[1]), 
                id=row[0], 
                description=f'Status:[Predicted];AssemblyAccession:[{row[2]}];CazyFamily:[{row[3]}];taxID:[{row[4]}];name:[{lineage["name"]}];species:[{lineage["species"]}];Group:[{lineage["targetGroup"]}]'
                )
                SeqIO.write(proteinRecord, f, "fasta")
                #print(f'>{row[0]} AssemblyAccession:[{row[2]}];CazyFamily:[{row[3]}];taxID:[{row[4]}];name:[{lineage["name"]}];species:[{lineage["species"]}];Group:[{lineage["targetGroup"]}]\n{row[1]}')

    resultsGetCharacterizedProteinsForFamily=session.execute(getCharacterizedProteinsForFamily)
    rows1=resultsGetCharacterizedProteinsForFamily.fetchall()
    print(f'There are {numberCharacterizedSequencesForFamily} characterized sequences for family {familyID} in DB, {len(rows1)} were retrieved from the MySQL db.')

    if rows1:
        with open(fileOutCharacterizedCazymeProteins, "w") as f:
            for row in rows1:
                lineage=getTaxInfo(taxID=row[5],password=password)
                if row[1]:
                    proteinRecord = SeqRecord(Seq(row[1]), 
                    id=row[0], 
                    description=f'Status:[{row[3]}];Database:[{row[2]}];CazyFamily:[{row[4]}];taxID:[{row[5]}];name:[{lineage["name"]}];species:[{lineage["species"]}];Group:[{lineage["targetGroup"]}]'
                    )
                    SeqIO.write(proteinRecord, f, "fasta")
                else:
                    print(f'No sequence for protein ID - characterized {row[0]}',file=sys.stderr)

    resultsGetStructureProteinsForFamily=session.execute(getStructureProteinsForFamily)
    rows2=resultsGetStructureProteinsForFamily.fetchall()
    print(f'There are {numberStructureSequencesForFamily} structure sequences for family {familyID} in DB, {len(rows2)} were retrieved from the MySQL db.')

    if rows2:
        with open(fileOutStructureCazymeProteins, "w") as f:
            for row in rows2:
                lineage=getTaxInfo(taxID=row[5],password=password)
                if row[1]:
                    proteinRecord = SeqRecord(Seq(row[1]), 
                    id=row[0], 
                    description=f'Status:[{row[3]}];Database:[{row[2]}];CazyFamily:[{row[4]}];taxID:[{row[5]}];name:[{lineage["name"]}];species:[{lineage["species"]}];Group:[{lineage["targetGroup"]}]'
                    )
                    SeqIO.write(proteinRecord, f, "fasta")
                else:
                    print(f'No sequence for protein ID - structure {row[0]}',file=sys.stderr)


def getTaxInfo(taxID=None,password=None):
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import text
    from sqlalchemy import select, update, bindparam
    from sqlalchemy.ext.automap import automap_base
 
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    taxonomicLineage = text("""with recursive ancestors as ( 
         select * from Taxonomy where TaxID=:taxID 
             union 
             select f.*  from Taxonomy as f, ancestors as a 
             where  f.TaxID = a.ParentTaxID 
             )
             select a.TaxName, a.RankName, a.TaxID 
             from ancestors as a, Taxonomy as b 
             where a.ParentTaxID = b.TaxID""")
    resultsGetTaxonomicLineage=session.execute(taxonomicLineage, {'taxID':taxID})
    rows=resultsGetTaxonomicLineage.fetchall()
    lineage={}
    if rows:
        targetGroup='Do not know - Problem with taxonomy'
        name=''
        for row in rows:
            # print(f'{row[0]} {row[1]} {row[2]}')
            lineage[row[1]]=row[0]
            if row[1]=='superkingdom' and row[0]=='Bacteria':
                targetGroup=row[0]
            elif row[1]=='superkingdom' and row[0]=='Archaea':
                targetGroup=row[0]
            elif row[1]=='kingdom' and row[0]=='Fungi':
                targetGroup=row[0]
            elif row[2] == taxID:
                name=row[0]
        lineage['targetGroup']=targetGroup
        lineage['name']=name
    return lineage

#Load dbCAN results in DB
def loadDbCANResults(password=None,pathDir=None):
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import text
    from sqlalchemy import select, update, bindparam
    from sqlalchemy.ext.automap import automap_base
    import sys
    import os
    import datetime
    import re
    import gzip
    from Bio import SeqIO
    import subprocess

    CAZymeClass={
    'GH':'Glycoside Hydrolases',
    'GT':'GlycosylTransferases',
    'PL':'Polysaccharide Lyases',
    'CE':'Carbohydrate Esterases',
    'AA':'Auxiliary Activities',
    'CBM':'Carbohydrate Binding Modules'
    }

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    GenomeFiles = Base.classes.GenomeFiles
    GenomeFileDownloaded = Base.classes.GenomeFileDownloaded
    Genomes = Base.classes.Genomes
    CazyFamilies = Base.classes.CazyFamilies
    ProteinSequence = Base.classes.ProteinSequences 
    Proteins2GenomeFile = Base.classes.Proteins2GenomeFile
    ProteinSequence2CazyFamily = Base.classes.ProteinSequence2CazyFamily

    getGenomeFileIDsQuery=text('''SELECT cc.ID FROM
(SELECT aa.ID FROM GenomeFiles as aa
 JOIN GenomeFileDownloaded as bb 
 ON aa.ID=bb.GenomeFileID
 where aa.FileType='Protein sequence'
 AND
 bb.Action='submitted dbCAN search') as cc
LEFT JOIN 
(SELECT dd.GenomeFileID FROM GenomeFileDownloaded as dd
WHERE dd.Action='Load dbCAN search') as ee
ON cc.ID=ee.GenomeFileID
WHERE ee.GenomeFileID is NULL''')

    #Get the list of GenomeFileID that appear to have been submitted to dbCAN search
    resultsGetGenomeFileIDs=session.execute(getGenomeFileIDsQuery)
    rows=resultsGetGenomeFileIDs.fetchall()
    countProts=0
    submittedGenomeFiles=[]
    if rows:
        for row in rows:
            getGenomeInfo=select([GenomeFiles.FileName,GenomeFiles.ID,Genomes.urlBase]).where(Genomes.AssemblyAccession==GenomeFiles.AssemblyAccession).where(GenomeFiles.ID==row[0])
            resultsGetGenomeInfo=session.execute(getGenomeInfo)
            data=resultsGetGenomeInfo.fetchone()
            if data:
                # print(f'{countIter} {data}')
                dirPath=pathDir+'/'+data[2].replace('https://ftp.ncbi.nlm.nih.gov/genomes/all/','')+'/'+data[0]
                fastagzPath=pathDir+'/'+data[2].replace('https://ftp.ncbi.nlm.nih.gov/genomes/all/','')+'/'+data[0]
                dirPath=dirPath.replace('.faa.gz','_dbCAN')
                loadSeqsFam=0
                if os.path.isdir(dirPath):
                    resFile=dirPath+'/overview.txt'
                    #Check if dbCAN results are available
                    if os.path.isfile(resFile):
                        #if dbCAN results available load protein sequence in SeqIO
                        if os.path.isfile(fastagzPath):
                            with gzip.open(fastagzPath, "rt") as fastaHandle:
                                fastaRecords = SeqIO.to_dict(SeqIO.parse(fastaHandle, "fasta"))
                        elif os.path.isfile(fastagzPath.replace('.gz','')):
                            fastaRecords = SeqIO.to_dict(SeqIO.parse(fastagzPath.replace('.gz',''), "fasta"))
                            subprocess.run(['gzip',fastagzPath.replace('.gz','')])
                        else:
                            print(f'No fasta file found in {dirPath}', file=sys.stderr)

                        #if dbCAN results are available insert action into DB
                        checkGenomeFileDownloaded=select(GenomeFileDownloaded).where(GenomeFileDownloaded.GenomeFileID==data[1]).where(GenomeFileDownloaded.Action=='Ran dbCAN search')
                        resultsCheckGenomeFileDownloaded=session.execute(checkGenomeFileDownloaded)
                        if resultsCheckGenomeFileDownloaded.fetchone() is None:
                            dateToday=datetime.date.today()
                            session.add(GenomeFileDownloaded(GenomeFileID=data[1],Action='Ran dbCAN search',ActionDate=dateToday))
                        with open(resFile, "r") as file:
                            for line in file:
                                if not line.startswith('Gene ID'):
                                    cazymes={}
                                    line=line.rstrip()
                                    protID,hmmerRes,HotpepRes,DiamondRes,nn=line.split('\t')
                                    if hmmerRes!='-':
                                        hmmerItems=hmmerRes.split('+')
                                        for hmmerItem in hmmerItems:
                                            hmmerItem=re.sub('\([0-9-]*\)','',hmmerItem).split('_')[0]
                                            # print(hmmerItem)
                                            if hmmerItem not in cazymes:
                                                cazymes[hmmerItem]={}
                                            cazymes[hmmerItem]['hmmer']=1
                                    if HotpepRes!='-':
                                        hotpepItems=HotpepRes.split('+')
                                        for hotpepItem in hotpepItems:
                                            hotpepItem=re.sub('\([0-9-]*\)','',hotpepItem).split('_')[0]
                                            # print(hotpepItem)
                                            if hotpepItem not in cazymes:
                                                cazymes[hotpepItem]={}
                                            cazymes[hotpepItem]['hotpep']=1
                                    if DiamondRes!='-':
                                        diamondItems=DiamondRes.split('+')
                                        for diamondItem in diamondItems:
                                            diamondItem=re.sub('\([0-9-]*\)','',hotpepItem).split('_')[0]
                                            # print(diamondItem)
                                            if diamondItem not in cazymes:
                                                cazymes[diamondItem]={}
                                            cazymes[diamondItem]['diamond']=1
                                    for fam in cazymes.keys():
                                        #The family should have been predicted by at least two tools
                                        if len(cazymes[fam])>1:
                                            countProts+=1
                                            if countProts % 100 == 0:
                                                session.commit()
                                            checkFamily=select(CazyFamilies).where(CazyFamilies.FamilyID==fam)
                                            resultsCheckFamily=session.execute(checkFamily)
                                            #If the family is not in DB, there are some 0 families e.g. GH0 predicted by dbCAN that are not present in CAZy, this will deal with that.
                                            if resultsCheckFamily.fetchone() is None:
                                                print(f'Adding family into DB: {CAZymeClass[re.sub("[0-9]*$","",fam)]} {fam}', file=sys.stderr)
                                                session.add(CazyFamilies(FamilyID=fam, FamilyClass=CAZymeClass[re.sub("[0-9]*$","",fam)]))
                                                # session.commit()
                                            checkProteinSequence=select(ProteinSequence).where(ProteinSequence.ProteinID==protID)
                                            resultsCheckProteinSequence=session.execute(checkProteinSequence)
                                            if resultsCheckProteinSequence.fetchone() is None:
                                                print(f'Adding protein sequence into DB: {protID}', file=sys.stderr)
                                                session.add(ProteinSequence(ProteinID=protID, Database='genbank',Sequence=fastaRecords[protID].seq))
                                                # session.commit()
                                            checkProteins2GenomeFile=select(Proteins2GenomeFile).where(Proteins2GenomeFile.GenomeFileID==data[1]).where(Proteins2GenomeFile.ProteinID==protID)
                                            resultsCheckProteins2GenomeFile=session.execute(checkProteins2GenomeFile)
                                            if resultsCheckProteins2GenomeFile.fetchone() is None:
                                                print(f'Adding protein to genome file into DB: {protID}', file=sys.stderr)
                                                session.add(Proteins2GenomeFile(GenomeFileID=data[1], ProteinID=protID))
                                                # session.commit()
                                            checkProteinSequence2CazyFamily=select(ProteinSequence2CazyFamily).where(ProteinSequence2CazyFamily.ProteinID==protID).where(ProteinSequence2CazyFamily.CazyFamilyID==fam)
                                            resultsCheckProteinSequence2CazyFamily=session.execute(checkProteinSequence2CazyFamily)
                                            if resultsCheckProteinSequence2CazyFamily.fetchone() is None:
                                                loadSeqsFam+=1
                                                print(f'Adding protein to family into DB: {protID} {fam}', file=sys.stderr)
                                                session.add(ProteinSequence2CazyFamily(ProteinID=protID, CazyFamilyID=fam))
                                                # session.commit()
                                            #print(f'{protID}\t{fastaRecords[protID].seq}\t{fam}\t{list(cazymes[fam].keys())}')
                                    #print(f'{protID},{hmmerRes},{HotpepRes},{DiamondRes}')
                        if loadSeqsFam>0:
                            checkGenomeFileDownloaded2=select(GenomeFileDownloaded).where(GenomeFileDownloaded.GenomeFileID==data[1]).where(GenomeFileDownloaded.Action=='Load dbCAN search')
                            resultsCheckGenomeFileDownloaded2=session.execute(checkGenomeFileDownloaded2)
                            if resultsCheckGenomeFileDownloaded2.fetchone() is None:
                                dateToday=datetime.date.today()
                                session.add(GenomeFileDownloaded(GenomeFileID=data[1],Action='Load dbCAN search',ActionDate=dateToday))
    else:
        sys.exit('No more files to process')
    #loadDbCANResults(password=password,pathDir=pathDir)
    session.commit()
    session.close()
#Run dbCAN on the protein file 
def submitCAZymeSearch(password=None,countIter=0,pathDir=None,maxGenomeID=0):
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import text
    from sqlalchemy import select, update, bindparam
    from sqlalchemy.ext.automap import automap_base
    import sys
    import os
    from shutil import which
    import datetime
    import time

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    GenomeFiles = Base.classes.GenomeFiles
    GenomeFileDownloaded = Base.classes.GenomeFileDownloaded
    Genomes = Base.classes.Genomes

#     getGenomeFileIDsQuery=text('''SELECT cc.ID FROM
# (SELECT aa.ID FROM GenomeFiles as aa
#  JOIN GenomeFileDownloaded as bb 
#  ON aa.ID=bb.GenomeFileID
#  where aa.FileType='Protein sequence'
#  AND
#  bb.Action='Downloaded') as cc
# LEFT JOIN 
# (SELECT dd.GenomeFileID FROM GenomeFileDownloaded as dd
# WHERE dd.Action='submitted dbCAN search') as ee
# ON cc.ID=ee.GenomeFileID
# WHERE ee.GenomeFileID is NULL limit 1000''')

    getGenomeFileIDsQuery=text('''SELECT cc.ID FROM
(SELECT aa.ID FROM GenomeFiles as aa
 JOIN GenomeFileDownloaded as bb 
 ON aa.ID=bb.GenomeFileID
 where aa.FileType='Protein sequence'
 AND
 bb.Action='Downloaded' 
 AND aa.ID >:maxID
 order by aa.ID) as cc
LEFT JOIN 
(SELECT dd.GenomeFileID FROM GenomeFileDownloaded as dd
WHERE dd.Action='submitted dbCAN search') as ee
ON cc.ID=ee.GenomeFileID
WHERE ee.GenomeFileID is NULL limit 1000''')

    resultsGetGenomeFileIDs=session.execute(getGenomeFileIDsQuery,params=dict(maxID=maxGenomeID))
    rows=resultsGetGenomeFileIDs.fetchall()
    # print(countIter)
    countRows=0
    submitGenomeFiles=[]
    if rows:
        listFilesfilename=f'listFiles.'+str(countIter)+'.txt'
        with open(listFilesfilename, 'w') as f:
            for row in rows:
                if row[0]>maxGenomeID:
                    maxGenomeID=row[0]
                getGenomeInfo=select([GenomeFiles.FileName,GenomeFiles.ID,Genomes.urlBase]).where(Genomes.AssemblyAccession==GenomeFiles.AssemblyAccession).where(GenomeFiles.ID==row[0])
                resultsGetGenomeInfo=session.execute(getGenomeInfo)
                data=resultsGetGenomeInfo.fetchone()
                if data:
                    # print(f'{countIter} {data}')
                    subDirs=pathDir+'/'+data[2].replace('https://ftp.ncbi.nlm.nih.gov/genomes/all/','')
                    filePath=subDirs+'/'+data[0]
                    if os.path.isfile(filePath):
                        # print(f'Processing {subDirs} {filePath}')
                        f.write(f'{filePath}\n')
                        submitGenomeFiles.append(data[1])
        submitScriptfilename=f'submitScript_runDbCAN.'+str(countIter)+'.sh'
        generateSubmissionScript(submitGenomeFiles,submitScriptfilename,listFilesfilename,countIter)
        if os.path.isfile(listFilesfilename) and os.path.isfile(submitScriptfilename):
            #submit to the cluster the dbCAN search
            #fisrt check if we have the qsub command
            if which('qsub') is not None:
                countRows+=1
                os.system(f'qsub {submitScriptfilename}')
                for id in submitGenomeFiles:
                    dateToday=datetime.date.today()
                    session.add(GenomeFileDownloaded(GenomeFileID=id, Action='submitted dbCAN search',ActionDate=dateToday)) #session.add(GenomeFileDownloaded(GenomeFileID=row[0], Action='submitted dbCAN search')) 
                    session.commit()
            else:
                print('qsub command not found..',file=sys.stderr)
        session.commit()
#        time.sleep(4)
    else:
        sys.exit('No more files to process')

    session.commit()
    session.close()    
    submitCAZymeSearch(password=password,countIter=countIter+1,pathDir=pathDir,maxGenomeID=maxGenomeID)

def generateSubmissionScript(listGenomeFiles=None,submitScriptfilename=None,listFilesfilename=None,countIter=None):
    with open(submitScriptfilename, 'w') as f:
        name=f'scriptdbCAN_{countIter}'
        previousInteger=countIter-1
        prevName=f'scriptdbCAN_{previousInteger}'
        f.write(f'#!/bin/bash\n#$ -cwd\n#$ -q all.q\n#$ -pe smp 4\n#$ -t 1-{len(listGenomeFiles)}\n#$ -tc 10\n')
        f.write(f'#$ -N {name}\n')
        f.write(f'#$ -hold_jid {prevName}\n')
        f.write(f'module load dbCAN/2.0.11\n')
        f.write(f'FILEPATH=$(head -n $SGE_TASK_ID {listFilesfilename} | tail -n 1)\n')
        f.write(f'BASEDIR=$(dirname $FILEPATH)\n')
        f.write(f'echo $FILEPATH\n')
        f.write(f'FILENAMEGZ=$(basename $FILEPATH)\n')
        f.write('FILENAME=${FILENAMEGZ/.gz}\n')
        f.write(f'cd $BASEDIR\n')
        f.write(f'gunzip $FILENAMEGZ\n')
        f.write('OUTDIR=${FILENAMEGZ/.faa.gz/_dbCAN}\n')
        f.write(f'rm -rf $OUTDIR\n')
        
        f.write('if test -f ${OUTDIR}/overview.txt; then\n')
        f.write('    echo "dbCAN already ran"\n')
        f.write('else\n')
        f.write(f'    run_dbcan.py --hmm_cpu $NSLOTS --hotpep_cpu $NSLOTS --tf_cpu $NSLOTS --stp_cpu $NSLOTS --dia_cpu $NSLOTS --out_dir $OUTDIR --db_dir /Storage/databases/dbCAN_V10/ $FILENAME protein\n')
        f.write('    echo "dbCAN search completed" > ${OUTDIR}/dbCAN.ok\n')
        f.write('fi\n')
        f.write(f'gzip $FILENAME\n')


def downloadGenomeFiles(password=None, dirPath=None, fileType=None):
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select, update, bindparam
    from sqlalchemy.ext.automap import automap_base
    import sys
    from os import path, mkdir, makedirs,remove
    import time
    import urllib.request
    import datetime
    import platform

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    Genomes = Base.classes.Genomes
    GenomeFiles = Base.classes.GenomeFiles
    GenomeFileDownloaded = Base.classes.GenomeFileDownloaded

    if path.exists(dirPath):
        print(f'Directory \'{dirPath}\' exists. OK.')	
    else:
        print(f'Directory \'{dirPath}\' does not exists. Creating it.')
        mkdir(dirPath)
    #Get the list of genomes that have not been downloaded yet, 50 at the time (recursive)
    getListOfGenomes2Download=select([Genomes.AssemblyAccession,Genomes.urlBase,GenomeFiles.FileName,GenomeFiles.ID]).join(GenomeFiles,Genomes.AssemblyAccession==GenomeFiles.AssemblyAccession).join(GenomeFileDownloaded,GenomeFiles.ID==GenomeFileDownloaded.GenomeFileID,isouter=True).where(GenomeFileDownloaded.ID==None).where(GenomeFiles.FileType=='Protein sequence').limit(100)
    print(getListOfGenomes2Download)
    resultGetListOfGenomes2Download=session.execute(getListOfGenomes2Download)
    rows=resultGetListOfGenomes2Download.fetchall()
    counterFiles2Download=0
    if(rows):
        for row in rows:
            subDirs=row[1].replace('https://ftp.ncbi.nlm.nih.gov/genomes/all/','')
            if platform.system() == 'Windows':
                subDirs=subDirs.replace('/','\\')
            elif platform.system() == 'Linux':
                subDirs=subDirs.replace('\\','/')
            urlFile=row[1]+'/'+row[2]
            completePath=path.join(dirPath,subDirs)
            completePathFile=path.join(completePath,row[2])
            completePathMD5File=path.join(completePath,'md5checksums.txt')
            urlMD5File=row[1]+'/md5checksums.txt'
            print(f'{urlFile}')
            if path.exists(completePath):
                print(f'Directory \'{completePath}\' exists. OK.')	
            else:
                print(f'Directory \'{completePath}\' does not exists. Creating it.')
                makedirs(completePath)
            if path.exists(completePathFile):
                print(f'Genome file {row[2]} exists', file=sys.stderr)
                md5sumLocalFile=computeMD5Sumfile(completePathFile)
                if path.exists(completePathMD5File):
                    md5sumRemoteFile=getMD5sumFromFile(completePathMD5File,row[2])
                else:
                    urllib.request.urlretrieve(urlMD5File, completePathMD5File)
                    md5sumRemoteFile=getMD5sumFromFile(completePathMD5File,row[2])
            else:
                counterFiles2Download+=1
                if counterFiles2Download % 10 == 0:
                    time.sleep(1)
                    session.commit()
                print(f'Genome  file {row[2]}  does not exist,  start downloading', file=sys.stdout)
                urllib.request.urlretrieve(urlFile, completePathFile)
                urllib.request.urlretrieve(urlMD5File, completePathMD5File)
                md5sumLocalFile=computeMD5Sumfile(completePathFile)
                md5sumRemoteFile=getMD5sumFromFile(completePathMD5File,row[2])
            if md5sumLocalFile != md5sumRemoteFile:
                print(f'MD5 checksum of file {row[2]} does not match. Deleting file.', file=sys.stderr)
                remove(completePathFile)
            else:
                print("Download complete", file=sys.stderr)
                getGenomeFileDownloaded=select([GenomeFileDownloaded.GenomeFileID]).where(GenomeFileDownloaded.Action=='Downloaded').where(GenomeFileDownloaded.GenomeFileID==row[3])
                resultsGetGenomeFileDownloaded=session.execute(getGenomeFileDownloaded)
                if resultsGetGenomeFileDownloaded.fetchone() is None:
                    print(f'Genome file {row[2]} downloaded. Inserting action \'Downloaded\' and date into DB.', file=sys.stdout)
                    dateToday=datetime.date.today()
                    session.add(GenomeFileDownloaded(GenomeFileID=row[3], Action='Downloaded', ActionDate=dateToday))
                else:
                    print(f'Genome file {row[2]} downloaded. Action \'Downloaded\' already in DB.', file=sys.stdout)
    else:
        sys.exit('No more files to process')

    session.commit()
    session.close()
    downloadGenomeFiles(password=password, dirPath=dirPath, fileType=fileType)


                    
    # #These are the files to download:
    # select c.*, a.ID, a.AssemblyAccession, a.FileType, b.urlBase, a.FileName
    #  from Genomes as b JOIN GenomeFiles as a
    #   ON a.AssemblyAccession=b.AssemblyAccession
    #   LEFT JOIN GenomeFileDownloaded as c
    #   ON a.ID=c.GenomeFileID 
    #   where a.FileType='Protein sequence'
    #   and c.GenomeFileID is null
    #   limit 10;

#Populate the ProteinSequences table - update, by getting the protein sequences from the Genbank/Uniprot and CAZy
def updateProteinSequences(password=None,apiKey=None):
    engine = connectDB(password)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select, update, bindparam
    from sqlalchemy.ext.automap import automap_base
    import sys

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    ProteinSequences = Base.classes.ProteinSequences

    updateStmt = (
        update(ProteinSequences).
        where(ProteinSequences.ProteinID == bindparam('proteinID')).
        where(ProteinSequences.Database == bindparam('DB')).
        values(Sequence=bindparam('sequence'))
    )

    getProteinSequences=select([ProteinSequences.ProteinID,ProteinSequences.Database]).where(ProteinSequences.Sequence==None).limit(100)
    resultsGetProteinSequences=session.execute(getProteinSequences)
    rows=resultsGetProteinSequences.fetchall()
    proteinIDsGenbank=[]
    proteinIDsUniprot=[]
    if rows:
        for row in rows:
            if row[1]=='genbank':
                proteinIDsGenbank.append(row[0])
                #sequence = getProteinSequenceFromGenbank(proteinID=row[0],apiKey=apiKey)
            elif row[1]=='uniprot':
                # sequence = getProteinSequenceFromUniprot(row[0])
                proteinIDsUniprot.append(row[0])
            else:
                print(f'Database {row[1]} not supported yet',file=sys.stderr)
        if len(proteinIDsGenbank)>0:
            # print(f'Updating {len(proteinIDsGenbank)} {proteinIDsGenbank} protein sequences from genbank',file=sys.stderr)	
            seqsGenbank=getProteinSequenceFromGenbank(proteinIDs=proteinIDsGenbank,apiKey=apiKey)
            # print(seqsGenbank,file=sys.stderr)
            if seqsGenbank:
                session.execute(updateStmt, seqsGenbank)
        if len(proteinIDsUniprot)>0:
            # print(f'Updating {len(proteinIDsUniprot)} protein sequences from uniprot',file=sys.stderr)
            seqsUniprot=getProteinSequenceFromUniprot(proteinIDs=proteinIDsUniprot)
            if seqsUniprot:
                session.execute(updateStmt, seqsUniprot)
                # session.commit()
    else:
        sys.exit('No more proteins to process')
        
    session.commit()
    session.close()
    updateProteinSequences(password=password, apiKey=apiKey)
    
#Get seqeunces from UniProt
def getProteinSequenceFromUniprot(proteinIDs=None):
    import sys
    from io import StringIO
    from Bio import SeqIO
    import requests
    import time
    
    seqsList=[]
    counter=0
    baseUrl="http://www.uniprot.org/uniprot/"

    for id in proteinIDs:
        counter+=1
        if counter%100==0:
            time.sleep(3)
        currentUrl=baseUrl+id+".fasta?version=*"
        #print(currentUrl)
        response = requests.post(currentUrl)
        cData=''.join(response.text)
        if cData:
            fastaIO=StringIO(cData)
            seqObj=SeqIO.parse(fastaIO,'fasta')
            seqsDict={}
            seqsDict['proteinID']=id
            seqsDict['DB']='uniprot'
            # print(seqsDict)
            seqsDict['sequence']=str(next(seqObj).seq)
            # print(seqsDict)
            # print(seqObj)
            seqsList.append(seqsDict)
            fastaIO.close()
        else:
            print(f'No sequence found for {id}',file=sys.stderr)
    return seqsList
    
#Get seqeunces from Genbank
def getProteinSequenceFromGenbank(proteinIDs, apiKey=None):
    import sys
    from io import StringIO
    from Bio import Entrez, SeqIO
    import re
    import time
    
    seqsList=[]
    if(apiKey):
        Entrez.email = "diego.riano@cena.usp.br"
        Entrez.api_key = apiKey
        searchRes = Entrez.read(Entrez.epost("protein", id=",".join(proteinIDs)))
        webenv = searchRes["WebEnv"]
        query_key = searchRes["QueryKey"]
        
        try:
            fastaIO = StringIO(Entrez.efetch(db="protein", rettype="fasta", retmode="text", webenv=webenv, query_key=query_key, api_key=apiKey).read())
        except:
            print(f'Error while getting sequences from Genbank for {proteinIDs}',file=sys.stderr)
            return seqsList
        
        seqsObj=SeqIO.parse(fastaIO,'fasta')
        for seq in seqsObj:
            # print(f'XXX:{seq.id}',file=sys.stderr)
            match=re.search(r'^[a-z]*\|+([A-Z0-9.]*)(\|.+)?$',seq.id,re.IGNORECASE)
            if match:
                # print('match')
                #Dealing with weird cases, where the accession retrieved is different. Some specific cases.
                if match.group(1) == '3PPS':
                    proteinID='333361328'
                elif match.group(1) == '4K3A':
                    proteinID='550545166'
                else:
                    #this deals with th enormal case, when the id of the retrieve seq should match wit the ID stored in the DB
                    proteinID=match.group(1)
            else:
                proteinID=seq.id
            seqsDict={}
            sequence=str(seq.seq)
            #if proteinID not in seqsDict:
            #    seqsDict[proteinID]={}
            seqsDict['proteinID']=proteinID
            seqsDict['DB']='genbank'
            seqsDict['sequence']=sequence
            seqsList.append(seqsDict)
            #updateProteinSequences(password=None,apiKey=apiKey,proteinID=proteinID,sequence=sequence)
        fastaIO.close()
        time.sleep(3)
        return seqsList
    else:
        print(f'No API key provided for Entrez. Please provide one in the command line.',file=sys.stderr)
#    print(f'Getting sequence for {proteinIDs} from Genbank')

#Populate Genomes table with data from NCBI genomes
def populateWebCAZyInfo(password=None,updateNCBITaxDB=False,infoFamily=None,enzymes=None,family=None,typePage=None):
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
    targetGroups={4751,2157,2,10239}

    if updateNCBITaxDB:#TODO. This is not working. Check it.
        ncbi.update_taxonomy_database()

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    CazyFamily = Base.classes.CazyFamilies
    CazyFamilyInfo = Base.classes.CazyFamilyInfo
    Taxonomy = Base.classes.Taxonomy
    StudiedCAZymes = Base.classes.StudiedCAZymes
    StudiedCAZymesPDB = Base.classes.StudiedCAZymesPDB
    StudiedCAZymesProteins = Base.classes.StudiedCAZymesProteins
    ProteinSequences = Base.classes.ProteinSequences
    ProteinReference = Base.classes.ProteinReferences
    ProteinEC = Base.classes.StudiedCAZymesEC


    for family in infoFamily:
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
            for key in infoFamily[family]:
                for item in infoFamily[family][key]:
                    item=unidecode(item)
                    #print(f'{key}\t{item}')
                    matchEC=re.search(r'\((EC [0-9.-]*)\)',item)
                    checkFamilyInfo=select([CazyFamilyInfo]).where(CazyFamilyInfo.FamilyID==family).where(CazyFamilyInfo.Key==key).where(CazyFamilyInfo.Value==item)
                    resultCheckFamilyInfo = session.execute(checkFamilyInfo)
                    if resultCheckFamilyInfo.fetchone():
                        True
                        # print(f'Already saw {family}\t{key}\t{item}')
                    else:
                        session.add(CazyFamilyInfo(FamilyID=family, Key=key, Value=item))
                    if matchEC:
                        checkFamilyInfoEC=select([CazyFamilyInfo]).where(CazyFamilyInfo.FamilyID==family).where(CazyFamilyInfo.Key=='Enzyme Code').where(CazyFamilyInfo.Value==matchEC.group(1))
                        resultCheckFamilyInfoEC = session.execute(checkFamilyInfoEC)
                        if resultCheckFamilyInfoEC.fetchone():
                            True
                            # print(f'Already saw {family}\tEnzyme Code\t{matchEC.group(1)}')
                        else:
                            session.add(CazyFamilyInfo(FamilyID=family, Key='Enzyme Code', Value=matchEC.group(1)))
            #commit the changes
            session.commit()
        else:
            print(f'{family}\tUnknown',file=sys.stderr)
    for inx in range(0,len(enzymes)):
        for name in enzymes[inx].keys():
            #Inserting Taxonomy info
            try:
                lineage = ncbi.get_lineage(int(enzymes[inx][name]['taxID']))
            except:
                print(f'Error: Missing TaxID from NCBI DB: {name}\n',file=sys.stderr)
                continue
            if targetGroups.intersection(set(lineage)):
                for index, i in enumerate(lineage):
                    checkTaxID=select([Taxonomy]).where(Taxonomy.TaxID==i)
                    resultCheckTaxID = session.execute(checkTaxID)
                    if i == 1:
                        #Check whether the taxonomy info is already in the DB. If not, add it
                        if resultCheckTaxID.fetchone() is None:
                            taxid2name = ncbi.get_taxid_translator([i])
                            rank = ncbi.get_rank([i])
                            #print(f'{index}\t{i}\t{lineage[index-1]}\t{lineage[index]}\t{taxid2name[i]}\t{rank[i]}')
                            #The root node is added to the DB, as it does not have parent and NA will be inserted for ParentTaxID
                            session.add(Taxonomy(TaxID=i, RankName=rank[i], TaxName=taxid2name[i])) 
                    else:
                        #Check whether the taxonomy info is already in the DB. If not, add it
                        if resultCheckTaxID.fetchone() is None:
                            taxid2name = ncbi.get_taxid_translator([i])
                            rank = ncbi.get_rank([i])
                            # print(f'{index}\t{i}\t{lineage[index-1]}\t{lineage[index]}\t{taxid2name[i]}\t{rank[i]}')
                            session.add(Taxonomy(ParentTaxID=lineage[index-1], TaxID=i, RankName=rank[i], TaxName=taxid2name[i]))
                session.commit()
                #Filling in StudiedCAZymes table
                StudiedCAZymesID=0
                checkStudiedCAZymes=select([StudiedCAZymes]).where(StudiedCAZymes.TaxID==int(lineage[-1])).where(StudiedCAZymes.TaxNameAsIs==enzymes[inx][name]['taxNameAsIs']).where(StudiedCAZymes.Name==unidecode(name)).where(StudiedCAZymes.FamilyID==family).where(StudiedCAZymes.Type==typePage)
                resultCheckStudiedCAZymes = session.execute(checkStudiedCAZymes)
                if resultCheckStudiedCAZymes.fetchone() is None:
                    if 'subFamily' in enzymes[inx][name].keys():
                        StudCazy=StudiedCAZymes(TaxID=int(lineage[-1]), subFamily=enzymes[inx][name]['subFamily'], TaxNameAsIs=enzymes[inx][name]['taxNameAsIs'], Name=unidecode(name), FamilyID=family, Type=typePage)
                    else:
                        StudCazy=StudiedCAZymes(TaxID=int(lineage[-1]), TaxNameAsIs=enzymes[inx][name]['taxNameAsIs'], Name=unidecode(name), FamilyID=family, Type=typePage)
                    session.add(StudCazy)
                    session.flush()
                    StudiedCAZymesID=StudCazy.StudiedCAZymesID
                else:
                    checkStudiedCAZymes2=select([StudiedCAZymes]).where(StudiedCAZymes.TaxID==int(lineage[-1])).where(StudiedCAZymes.TaxNameAsIs==enzymes[inx][name]['taxNameAsIs']).where(StudiedCAZymes.Name==unidecode(name)).where(StudiedCAZymes.FamilyID==family).where(StudiedCAZymes.Type==typePage)
                    resultCheckStudiedCAZymes2 = session.execute(checkStudiedCAZymes2)
                    StudiedCAZymesID=resultCheckStudiedCAZymes2.fetchone().StudiedCAZymes.StudiedCAZymesID
                #Filling in StudiedCAZymesPDB table
                for pdb in enzymes[inx][name]['PDB']:
                    checkStudiedCAZymesPDB=select([StudiedCAZymesPDB]).where(StudiedCAZymesPDB.StudiedCAZymesID==StudiedCAZymesID).where(StudiedCAZymesPDB.PDBID==pdb).where(StudiedCAZymesPDB.PDBChain==enzymes[inx][name]['PDB'][pdb])
                    resultCheckStudiedCAZymesPDB = session.execute(checkStudiedCAZymesPDB)
                    if resultCheckStudiedCAZymesPDB.fetchone() is None:
                        StudCazyPDB=StudiedCAZymesPDB(StudiedCAZymesID=StudiedCAZymesID, PDBID=pdb, PDBChain=enzymes[inx][name]['PDB'][pdb])
                        session.add(StudCazyPDB)
                        session.commit()
                if 'seqAccsGenBank' in enzymes[inx][name].keys():
                    for acc in enzymes[inx][name]['seqAccsGenBank']:
                        checkProteinSequences=select([ProteinSequences]).where(ProteinSequences.ProteinID==acc)
                        resultCheckProteinSequences = session.execute(checkProteinSequences)
                        if resultCheckProteinSequences.fetchone() is None:
                            ProteinSequence=ProteinSequences(ProteinID=acc, Database='genbank')
                            session.add(ProteinSequence)
                            session.commit()
                        checkStudiedCAZymesProteins=select([StudiedCAZymesProteins]).where(StudiedCAZymesProteins.StudiedCAZymesID==StudiedCAZymesID).where(StudiedCAZymesProteins.ProteinID==acc)
                        resultCheckStudiedCAZymesProteins=session.execute(checkStudiedCAZymesProteins)
                        if resultCheckStudiedCAZymesProteins.fetchone() is None:
                            StudCazyProtein=StudiedCAZymesProteins(StudiedCAZymesID=StudiedCAZymesID, ProteinID=acc)
                            session.add(StudCazyProtein)
                            session.commit()
                if 'seqAccsUniprot' in enzymes[inx][name].keys():
                    for acc in enzymes[inx][name]['seqAccsUniprot']:
                        checkProteinSequences=select([ProteinSequences]).where(ProteinSequences.ProteinID==acc)
                        resultCheckProteinSequences = session.execute(checkProteinSequences)
                        if resultCheckProteinSequences.fetchone() is None:
                            ProteinSequence=ProteinSequences(ProteinID=acc, Database='uniprot')
                            session.add(ProteinSequence)
                            session.commit()
                        checkStudiedCAZymesProteins=select([StudiedCAZymesProteins]).where(StudiedCAZymesProteins.StudiedCAZymesID==StudiedCAZymesID).where(StudiedCAZymesProteins.ProteinID==acc)
                        resultCheckStudiedCAZymesProteins=session.execute(checkStudiedCAZymesProteins)
                        if resultCheckStudiedCAZymesProteins.fetchone() is None:
                            StudCazyProtein=StudiedCAZymesProteins(StudiedCAZymesID=StudiedCAZymesID, ProteinID=acc)
                            session.add(StudCazyProtein)
                            session.commit()
                if 'References' in enzymes[inx][name].keys():
                    for id in enzymes[inx][name]['References']:
                        # print(f'{StudiedCAZymesID}\t{id}\t{enzymes[inx][name]["References"][id]}')
                        checkReferences=select([ProteinReference]).where(ProteinReference.StudiedCAZymesID==StudiedCAZymesID).where(ProteinReference.Reference==id)
                        resultCheckReferences = session.execute(checkReferences)
                        if resultCheckReferences.fetchone() is None:
                            Reference=ProteinReference(StudiedCAZymesID=StudiedCAZymesID, Reference=id, Source=enzymes[inx][name]['References'][id])
                            session.add(Reference)
                            session.commit()
                if 'ec' in enzymes[inx][name].keys():
                    for id in enzymes[inx][name]['ec']:
                        # print(f'{StudiedCAZymesID}\t{id}\t{enzymes[inx][name]["ec"][id]}')
                        checkProteinEC=select([ProteinEC]).where(ProteinEC.StudiedCAZymesID==StudiedCAZymesID).where(ProteinEC.EC==id)
                        resultCheckProteinECs = session.execute(checkProteinEC)
                        if resultCheckProteinECs.fetchone() is None:
                            ECs=ProteinEC(StudiedCAZymesID=StudiedCAZymesID, EC=id)
                            session.add(ECs)
                            session.commit()                                

            #print(enzymes[inx][name]['taxID'])
            #print(enzymes[inx])
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
    from os.path import exists, getmtime
    import sys

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
    if exists('genomes.txt'):
        print(f'Genome info file from NCBI exists,  checking age', file=sys.stderr)
        mtime=getmtime('genomes.txt')
        now=time.time()
        if (now - mtime) > 604800: #Download the file again if the file in disk is older than 7 days (60*60*24*7)
            print(f'Genome info file from NCBI is older than 7 days, downloading', file=sys.stderr)
            urllib.request.urlretrieve(url, 'genomes.txt')
            print("Download complete", file=sys.stderr)
        else:
            print(f'Genome info file from NCBI is younger than 7 days, not downloading and processing as it is', file=sys.stderr)
    else:
        print(f'Genome info file from NCBI does not exist,  start downloading', file=sys.stderr)
        urllib.request.urlretrieve(url, 'genomes.txt')
        print("Download complete", file=sys.stderr)

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
                    # print(f'Already saw {fields[assemblyAccessionIndex]}')
                    continue
                else:
                    True
                    # print(f'Checking {fields[assemblyAccessionIndex]}')

                #Commit to the DB every 100 lines of the genome file
                if counter % 100 == 0:
                    session.commit()

                #If there is no taxonomy info for the genome, control the error and continue
                try:
                    lineage = ncbi.get_lineage(int(fields[1]))
                except:
                    errorsLog.write(f'Error: Missing TaxID from NCBI DB: {fields[1]} for assembly: {fields[assemblyAccessionIndex]}\n')
                    continue
                #Only processes genomes with a taxonomy ID in fungi, archaea or bacteria
                targetGroups={4751,2157,2,10239}
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
                    #Check which files are available for the genome.
                    if resultCheckGenome.fetchone() is None:
                        acc,ver=fields[assemblyAccessionIndex].split('.')
                        if len(acc) == 13:
                            FTP_USER = "anonymous"
                            FTP_PASS = ""
                            FTP_HOST = "ftp.ncbi.nlm.nih.gov"
                            try:
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
                            except:
                                errorsLog.write(f'Error: Could not download files for assembly: {fields[assemblyAccessionIndex]}\n')
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