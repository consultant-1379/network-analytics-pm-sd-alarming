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
# Name    : ModifySelectedCollection.py
# Date    : 17/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#

from System import Environment, Threading
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data import DataType
import clr
clr.AddReference('System.Data')
from System import Array, String
from System.Data.Odbc import OdbcConnection, OdbcCommand
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

# global vars
conn_string_eniq = "DSN=" + Document.Properties['ENIQDB'] + ";Pooling=true;Max Pool Size=20;Enlist=true;FetchArraySize=100000;"


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
    CollectionType = 'CollectionType'
    EniqDs = 'EniqName'
    Expression = 'WildcardExpression'
    WildCardCollectionName = 'WildcardCollectionName'
	
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


																																   

def writeNodeCollectionToDB(nodeCollection, collectionName, collectionType, nodeType, systemArea, EniqDs, collectionID,typeOfCollection):
    if typeOfCollection == 'Static':
		sqlUpdateStatement = """
			Update "tblCollection" SET
				"CollectionName"  = '{0}',
				"NodeType" = '{1}',
				"SystemArea" = '{2}',
				"CollectionType" = '{3}',
				"TypeOfCollection"='{6}',
				"EniqID" = (select "EniqID" from "tblEniqDS" where "EniqName"='{4}')
			where "CollectionID" = {5};
			""".format(collectionName,nodeType,systemArea,collectionType,EniqDs,collectionID,typeOfCollection)

		sqlDelete = """
			Delete from "tblNode" where "CollectionID" = {0};
			""".format(collectionID)

		nodeStatementList = []
		for node in nodeCollection:
			nodeStatementList.append("""
				INSERT INTO "tblNode" ("CollectionID", "NodeName") VALUES ({0},'{1}');
				""".format(collectionID,node))
    elif typeOfCollection == 'Dynamic':
		sqlUpdateStatement = """
			Update "tblCollection" SET
				"CollectionName"  = '{0}',
				"NodeType" = '{1}',
				"SystemArea" = '{2}',
				"CollectionType" = '{3}',
				"TypeOfCollection"='{6}',
				"EniqID" = (select "EniqID" from "tblEniqDS" where "EniqName"='{4}')
			where "CollectionID" = {5};
			""".format(collectionName,nodeType,systemArea,collectionType,EniqDs,collectionID,typeOfCollection)

		sqlDelete = """
			Delete from "tblNode" where "CollectionID" = {0};
			""".format(collectionID)
			
		nodeStatementList = []
		for node in nodeCollection:
			tmp_node = node.replace("'", "''")
			nodeStatementList.append("""
				INSERT INTO "tblNode" ("CollectionID", "WildCardDefinition","NodeName") VALUES ({0},'{1}','');
				""".format(collectionID,tmp_node))
	
													

    sqlInsertNodeTemplate = ' '.join(nodeStatementList)
																		  

    #combine statements together
    sql = sqlUpdateStatement + " " +  sqlDelete + " " + sqlInsertNodeTemplate

    print(sql)
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
        Document.Properties['ConnectionError'] = "** Error when saving collection"
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


def createCursor(eTable):
    CursList = []
    ColList = []
    colname = []
    for eColumn in eTable.Columns:
        CursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        ColList.append(eTable.Columns[eColumn.Name].ToString())
    CursArray = Array[DataValueCursor](CursList)
    cusrDict = dict(zip(ColList, CursList))
    return cusrDict 


CollectionName = Document.Properties['CollectionName']
CollectionTable = Document.Data.Tables['NodeCollection']
cursor = DataValueCursor.CreateFormatted(CollectionTable.Columns["CollectionName"])

#create column names from class
NodeCollectionColumn = NodeCollectionColumn()
nodeCollectionColumns = [NodeCollectionColumn.CollectionName, NodeCollectionColumn.NodeName, NodeCollectionColumn.SystemArea, NodeCollectionColumn.EniqDs,NodeCollectionColumn.Expression,NodeCollectionColumn.WildCardCollectionName]
																																				 

nodeCollectionsErrorPropertyName = 'NodeCollectionError'
Document.Properties[nodeCollectionsErrorPropertyName] = ''


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

EniqDS =  Document.Properties['ENIQDataSourcesDropDown']

staticOrDynamic = Document.Properties['DynamicCollection']


if staticOrDynamic == 'OFF':
	nodes = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.NodeName)

	#getting distinct val, there will be only one for nodeType & system area, so just use first val
	nodeType = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.NodeType)
	systemArea = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.SystemArea)
	EniqDs = Document.Properties['ENIQDataSourcesDropDown']#getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, NodeCollectionColumn.EniqDs)
	print EniqDs 

	collectionIDFound = False
	if len(nodes) >0:
		selectedCollection = nodeCollectionsDataTable.Select("[CollectionName]= '" + CollectionName + "'")
		for collection in nodeCollectionsDataTable.GetRows(selectedCollection.AsIndexSet(),Array[DataValueCursor](nodeCollectionsCur.values())):

			if CollectionName == nodeCollectionsCur['CollectionName'].CurrentValue:
				collectionID = nodeCollectionsCur['CollectionID'].CurrentValue
				collectionIDFound = True

			else:
				collectionIDFound = False

		if collectionIDFound:
			if (writeNodeCollectionToDB(nodes,CollectionName,'Collection', nodeType[0], systemArea[0], EniqDs, collectionID,'Static')):
				Document.Properties['ConnectionError'] = "** Collection Created Successfully"
				changeView()

			Document.Properties[nodeCollectionsErrorPropertyName] = ''  # clear any existing warning
			Document.Properties["selectedNodes"] = ""
			Document.Data.Tables[nodeCollectionsDataTableName].Refresh()
	else:
		Document.Properties['ConnectionError'] = "** Select at least one node before saving collection."
else:
	
	Expression = getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.Expression)

	#getting distinct val, there will be only one for nodeType & system area, so just use first val
	nodeType = getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.NodeType)
	systemArea = getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.SystemArea)
	EniqDs = Document.Properties['ENIQDataSourcesDropDown']#getDistinctValuesList(wildcardCollectionDataTable, wildcardCur, NodeCollectionColumn.EniqDs)

	collectionIDFound = False
	if len(Expression) >0:
		selectedCollection = nodeCollectionsDataTable.Select("[CollectionName]= '" + CollectionName + "'")
		for collection in nodeCollectionsDataTable.GetRows(selectedCollection.AsIndexSet(),Array[DataValueCursor](nodeCollectionsCur.values())):

			if CollectionName == nodeCollectionsCur['CollectionName'].CurrentValue:
				collectionID = nodeCollectionsCur['CollectionID'].CurrentValue
				collectionIDFound = True

			else:
				collectionIDFound = False
  
		if collectionIDFound:
					 
			if (writeNodeCollectionToDB(Expression,CollectionName,'Collection', nodeType[0], systemArea[0], EniqDs, collectionID,'Dynamic')):
				Document.Properties['ConnectionError'] = "** Collection Created Successfully"
				changeView()

			Document.Properties[nodeCollectionsErrorPropertyName] = ''  # clear any existing warning
			Document.Properties["selectedNodes"] = ""
			Document.Data.Tables[nodeCollectionsDataTableName].Refresh()
	else:
		Document.Properties['ConnectionError'] = "** Select at least one node before saving collection."
  

Document.Properties["SelectedCollectionToModify"] = None
Document.Properties["SystemAreaErrorMsg"] = ""