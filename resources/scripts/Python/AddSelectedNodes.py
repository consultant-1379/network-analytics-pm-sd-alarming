# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2020 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : AddSelectedNodes.py
# Date    : 17/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarming
#
from System import Environment
from datetime import date
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
import clr
clr.AddReference('System.Data')
from System import Environment, Threading
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System.Data.Odbc import OdbcConnection, OdbcCommand

from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

username = Threading.Thread.CurrentPrincipal.Identity.Name

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)

notify = Application.GetService[NotificationService]()


def _from_bytes(bts):
    return [ord(b) for b in bts]

def _from_hex_digest(digest):
    return [int(digest[x:x+2], 16) for x in xrange(0, len(digest), 2)]


def decrypt(data, digest=True):
    '''
    Performs decrypting of provided encrypted data. 
    If 'digest' is True data must be hex digest, otherwise data should be
    encrtypted bytes.
    
    This function is simetrical with encrypt function.
    '''
    try:
        data = Array[Byte](map(Byte, _from_hex_digest(data) if digest else _from_bytes(data)))
        
        rm = RijndaelManaged()
        dec_transform = rm.CreateDecryptor(_key, _vector)
    
        mem = MemoryStream()
        cs = CryptoStream(mem, dec_transform, CryptoStreamMode.Write)
        cs.Write(data, 0, data.Length)
        cs.FlushFinalBlock()
        
        mem.Position = 0
        decrypted = Array.CreateInstance(Byte, mem.Length)
        mem.Read(decrypted, 0, decrypted.Length)
        
        cs.Close()
        utfEncoder = UTF8Encoding()
        return utfEncoder.GetString(decrypted)

    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)

try:
    connString = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)


class NodeCollectionColumn:
    CollectionName = 'CollectionName'
    NodeName = 'NodeName'
    NodeType = 'NodeType'
    SystemArea = 'SystemArea'
    CollectionType = 'TypeOfCollection'
    #Typ='TypeOfCollection'
    Expression = 'WildcardExpression'
    #WildCardCollectionName = 'WildcardCollectionName'


def createTable(dataTableName, stream):
    settings = TextDataReaderSettings()
    settings.Separator = ";"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)
    settings.SetDataType(0, DataType.String)
    settings.SetDataType(1, DataType.String)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)
    if Document.Data.Tables.Contains(dataTableName):
        Document.Data.Tables[dataTableName].ReplaceData(fs)
    else:
        Document.Data.Tables.Add(dataTableName, fs)


def checkEmpty(*args):
    response = []
    if len(args) != 0:
        for item in args:
            if item == "":
                response.append(True)
            else:
                response.append(False)
    return response


def createCursor(eTable):
    """Create cursors for a given table, these are used to loop through columns
    """
    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))
    return cusrDict


def getDistinctValuesList(dataTable, cursor, specifiedColumn):
    listValues = []
    rows = IndexSet(dataTable.RowCount, True)
    for row in dataTable.GetDistinctRows(rows,cursor[specifiedColumn]):
            listValues.append(cursor[specifiedColumn].CurrentValue)
    return listValues


def writeNodeCollectionToDB(nodeOrExpression, collectionName, collectionType, NodeType, systemArea, CreatedBy, CreatedOn, EniqDs, connString,TypeOfCollection):
    nodeStatementList = []
    

    if "PostgreSQL" in connString:
        if TypeOfCollection == 'Static':
            sqlInsertCollectionTemplate = """
                INSERT INTO "tblCollection" ("CollectionName", "NodeType", "SystemArea", "CollectionType", "CreatedBy", "CreatedOn", "EniqID","TypeOfCollection")
                    VALUES (
                        '{0}',
                        '{1}',
                        '{2}',
                        '{3}',
                        '{4}',
                        '{5}',
                        (select "EniqID" from "tblEniqDS" where "EniqName" = '{6}'),
                        '{7}'
                    );
                """.format(collectionName,NodeType,systemArea,collectionType,CreatedBy,CreatedOn, EniqDs,TypeOfCollection)
            for node in nodeOrExpression:
                nodeStatementList.append("""
                    INSERT INTO "tblNode" ("CollectionID", "NodeName") VALUES (currval('"tblCollection_seq"'),'{0}');
                    """.format(node))
        elif TypeOfCollection == 'Dynamic':
            sqlInsertCollectionTemplate = """
                INSERT INTO "tblCollection" ("CollectionName", "NodeType", "SystemArea", "CollectionType", "CreatedBy", "CreatedOn", "EniqID","TypeOfCollection")
                    VALUES (
                        '{0}',
                        '{1}',
                        '{2}',
                        '{3}',
                        '{4}',
                        '{5}',
                        (select "EniqID" from "tblEniqDS" where "EniqName" = '{6}'),
                        '{7}'
                    );
                """.format(collectionName,NodeType,systemArea,collectionType,CreatedBy,CreatedOn, EniqDs,TypeOfCollection)
            for node in nodeOrExpression:
                nodeStatementList.append("""
                    INSERT INTO "tblNode" ("CollectionID", "WildCardDefinition","NodeName") VALUES (currval('"tblCollection_seq"'),'{0}','');
                    """.format(node.replace("'", "''")))
                break
    
    else:
        if TypeOfCollection == 'Static':
            sqlInsertCollectionTemplate = """
                INSERT INTO tblCollection (collectionName, nodeType, systemArea, collectionType, CreatedBy, CreatedOn, EniqID,TypeOfCollection)
                    VALUES (
                        '{0}',
                        '{1}',
                        '{2}',
                        '{3}',
                        '{4}',
                        '{5}',
                        (select "EniqID" from "tblEniqDS" where "EniqName" = '{6}'),
                        '{7}'
                    )
                DECLARE @colID int
                SET @colID = SCOPE_IDENTITY()
                """.format(collectionName,NodeType,systemArea,collectionType,CreatedBy,CreatedOn, EniqDs,TypeOfCollection)      
            for node in nodeOrExpression:
                nodeStatementList.append("""
                    INSERT INTO dbo.tblNode (collectionID, nodeName) VALUES (@colID,'{0}')
                    """.format(node))
        elif TypeOfCollection == 'Dynamic':
            sqlInsertCollectionTemplate = """
                INSERT INTO tblCollection (collectionName, nodeType, systemArea, collectionType, CreatedBy, CreatedOn, EniqID,TypeOfCollection)
                    VALUES (
                        '{0}',
                        '{1}',
                        '{2}',
                        '{3}',
                        '{4}',
                        '{5}',
                        (select "EniqID" from "tblEniqDS" where "EniqName" = '{6}'),
                        '{7}'
                    )
                DECLARE @colID int
                SET @colID = SCOPE_IDENTITY()
                """.format(collectionName,NodeType,systemArea,collectionType,CreatedBy,CreatedOn, EniqDs,TypeOfCollection)      
            for node in nodeOrExpression:
                nodeStatementList.append("""
                    INSERT INTO dbo.tblNode (collectionID,WildCardDefinition,nodeName) VALUES (@colID,'{0}','')
                    """.format(node.replace("'", "''")))
                break
    
    sqlInsertNodeTemplate = ' '.join(nodeStatementList)
    

    #combine two statements together
    sql = sqlInsertCollectionTemplate + " " + sqlInsertNodeTemplate
    print sql
    return (writeToDB(sql))



def writeToDB(sql):

    try:
        connection = OdbcConnection(connString)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command.ExecuteReader()
        connection.Close()
        return True
    except Exception as e:
        print (e.message)
        Document.Properties['ActionMessage'] = "** Error when saving collection"
        return False

def changeView():

    tableName = 'NodeList'
    stream = MemoryStream()
    csvWriter = StreamWriter(stream)
    csvWriter.WriteLine("node;NodeType\r\n")
    csvWriter.Flush()
    createTable(tableName, stream)

    for page in Document.Pages:
        if (page.Title == 'Node Collection Manager'):
            Document.ActivePageReference=page


#create column names from class
NodeCollectionColumn = NodeCollectionColumn()
nodeCollectionColumns = [NodeCollectionColumn.CollectionName, NodeCollectionColumn.NodeName, NodeCollectionColumn.SystemArea, NodeCollectionColumn.Expression]

nodeCollectionsErrorPropertyName = 'NodeCollectionError'
Document.Properties[nodeCollectionsErrorPropertyName] = ''

CollectionName = Document.Properties['CollectionName']

# create cursors for NodeCollection table
nodeCollectionsDataTableName = 'NodeCollection'
nodeCollectionsDataTable = Document.Data.Tables[nodeCollectionsDataTableName]
nodeCollectionsCur = createCursor(nodeCollectionsDataTable)

#create cursor for selectedNode table
selectedNodeDataTableName = 'SelectedNodes'
selectedNodeDataTable = Document.Data.Tables[selectedNodeDataTableName]
selectedNodeCur = createCursor(selectedNodeDataTable)

#create cursor for Wildcardcollection table
wildcardCollectionTable = 'Wildcard Collections'
wildcardCollectionDataTable = Document.Data.Tables[wildcardCollectionTable]
wildcardCur = createCursor(wildcardCollectionDataTable)
EniqDs = Document.Properties['ENIQDataSourcesDropDown']


staticOrDynamic = Document.Properties['Dynamiccollection']

if staticOrDynamic == 'OFF':
    #check if collection name already exists
    nodeCollections = getDistinctValuesList(nodeCollectionsDataTable, nodeCollectionsCur, NodeCollectionColumn.CollectionName)
    newCollectionName = Document.Properties[NodeCollectionColumn.CollectionName.replace('_', '')]
    TypeOfCollection = 'Static'
    
    nodes = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.NodeName)

    if newCollectionName in nodeCollections:
        Document.Properties[nodeCollectionsErrorPropertyName] = 'Error: Collection Name "%s" already exists' % newCollectionName
        print "Collection Name %s already exists" % newCollectionName
        Document.Properties['ActionMessage'] = "** Collection Already Exists"
    else:
        Document.Properties[nodeCollectionsErrorPropertyName] = ''  # clear any existing warning
        nodes = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.NodeName)
        
        if checkEmpty(Document.Properties['CollectionName'])[0] == False and Document.Properties['NodeType'] not in ['',None,'(Empty)','( Empty)','None'] and Document.Properties['SystemArea'] not in ['',None,'(Empty)','( Empty)','None']:
            if len(nodes) != 0:
                #getting distinct val, there will be only one for nodeType & system area, so just use first val
                #nodeType = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.NodeType)
                #systemArea = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.SystemArea)
                nodeType = Document.Properties['NodeType']
                systemArea = Document.Properties['SystemArea']
                CreatedBy =  username
                CreatedOn = str(date.today())
                EniqDS = Document.Properties['ENIQDataSourcesDropDown']

                #write node list to database
                if (writeNodeCollectionToDB(nodes,CollectionName,'Collection', nodeType, systemArea, CreatedBy, CreatedOn, EniqDS, connString,TypeOfCollection)):
                    Document.Properties['ActionMessage'] = "** Collection Created Successfully"
                    changeView()
            else:
                Document.Properties['ActionMessage'] = "** Add nodes to create collection (SelectedNodes Cannot be Blank)"
        else:
            Document.Properties['ActionMessage'] = "Collection name, System Area and Node Type cannot be blank"
else:
    #check if collection name already exists
    nodeCollections = getDistinctValuesList(nodeCollectionsDataTable, nodeCollectionsCur, NodeCollectionColumn.CollectionName)
    newCollectionName = Document.Properties[NodeCollectionColumn.CollectionName.replace('_', '')]
    TypeOfCollection = 'Dynamic'

    if newCollectionName in nodeCollections:
        Document.Properties[nodeCollectionsErrorPropertyName] = 'Error: Collection Name "%s" already exists' % newCollectionName
        print "Collection Name %s already exists" % newCollectionName
        Document.Properties['ActionMessage'] = "** Collection Already Exists"
    else:
        Document.Properties[nodeCollectionsErrorPropertyName] = ''  # clear any existing warning
        Expression = getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.Expression)
        
        if checkEmpty(Document.Properties['CollectionName'])[0] == False and Document.Properties['NodeType'] not in ['',None,'(Empty)','( Empty)','None'] and Document.Properties['SystemArea'] not in ['',None,'(Empty)','( Empty)','None']:
            if len(Expression) != 0:
                #getting distinct val, there will be only one for nodeType & system area, so just use first val
                #nodeType = getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.NodeType)
                #systemArea = getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.SystemArea)
                nodeType = Document.Properties['NodeType']
                systemArea = Document.Properties['SystemArea']
                CreatedBy =  username
                CreatedOn = str(date.today())
                EniqDS = Document.Properties['ENIQDataSourcesDropDown']

                #write node list to database
                if (writeNodeCollectionToDB(Expression,CollectionName,'Collection', nodeType, systemArea, CreatedBy, CreatedOn, EniqDS, connString,TypeOfCollection)):
                    Document.Properties['ActionMessage'] = "** Collection Created Successfully"
                    changeView()
            else:
                Document.Properties['ActionMessage'] = "** Add expression to create collection (wildcardexpression Cannot be Blank)"
        else:
            Document.Properties['ActionMessage'] = "Collection name, System Area and Node Type cannot be blank"
        

#refresh tables/parameters
Document.Data.Tables[nodeCollectionsDataTableName].Refresh()
Document.Properties["selectedNodes"] = ""
Document.Properties["SystemAreaErrorMsg"] = ""
Document.Properties["SelectedCollectionToModify"] = None