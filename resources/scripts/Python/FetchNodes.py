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
# Name    : FetchNodes.py
# Date    : 17/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#

import clr
clr.AddReference('System.Data')
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Framework.ApplicationModel import *
from Spotfire.Dxp.Data import *
from System import Array
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcDataAdapter
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import CalculatedColumn
import time


def createTable(dataTableName, textData):
    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine("node;NodeType;SystemArea\r\n")
    writer.Flush()

    for line in textData:
        writer.WriteLine(line)
    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = ";"
    settings.AddColumnNameRow(0)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(dataTableName):
        dataTable = Document.Data.Tables[dataTableName]
        # clear table

        dataTable.RemoveRows(RowSelection(IndexSet(dataTable.RowCount,True)))
        settings = AddRowsSettings(dataTable, fs)
        dataTable.AddRows(fs, settings)
    else:
        # Create Table if not already present
        dataTable = Document.Data.Tables.Add(dataTableName, fs)

def createCursor(eTable):
    CursList = []
    ColList = []
    colname = []

    for eColumn in eTable.Columns:
        CursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        ColList.append(eTable.Columns[eColumn.Name].ToString())
    CursArray = Array[DataValueCursor](CursList)
    cusrDict=dict(zip(ColList,CursList))

    return cusrDict


    
def getTopologyTableData_wildcard(nodeType, dataS):

    topologyTableName = 'Modified Topology Data'
    topologyDataTable = Document.Data.Tables[topologyTableName]
    topologyDataTableCur = createCursor(topologyDataTable)
    
                                                                              

    selectedNodeType = topologyDataTable.Select("[Node]= '" + nodeType + "' and [DataSourceName] = '" + dataS + "'")
    print nodeType, dataS
    for node in topologyDataTable.GetRows(selectedNodeType.AsIndexSet(), Array[DataValueCursor](topologyDataTableCur.values())):
        #serverName = topologyDataTableCur['DataSourceName'].CurrentValue
        #if serverName == dataS:
             
        tableName = topologyDataTableCur['Topology Table'].CurrentValue
        FDNName = topologyDataTableCur['FDN Key'].CurrentValue
        KeyName = topologyDataTableCur['Key'].CurrentValue
        print tableName


    return tableName + ',' + FDNName + ',' + KeyName


def CreateSQL(nodeType,systemArea,wildcardCollectionName,wildcardExpression,dataSourceName):
    if '_' in wildcardExpression:
        wildcardExpressionFinal = wildcardExpression.replace("_","[_]")
    else:
        wildcardExpressionFinal = wildcardExpression
    alldata = getTopologyTableData_wildcard(nodeType,dataSourceName).split(',')
    tableName = alldata[0]
    FDNName = alldata[1]
    KeyName = alldata[2]
    sql = """
    SELECT DISTINCT 
        {0} as NodeName,
        {1} as FDN, 
        '{2}' AS NodeType, 
        '{3}' as SystemArea,
        '{4}' as CollectionName,
        '{5}' as WildcardExpression
    FROM 
        {6}
    WHERE  {7}""".format(KeyName,FDNName,nodeType,systemArea,wildcardCollectionName, wildcardExpression.replace("'", "''"),tableName, wildcardExpressionFinal)
    return sql     
    
def getTopologyTableData(nodeType, systemArea):

    topologyTableName = 'Topology Data'
    topologyDataTable = Document.Data.Tables[topologyTableName]
    topologyDataTableCur = createCursor(topologyDataTable)

    selectedNodeType = topologyDataTable.Select("[Node]= '" + nodeType + "'")

    for node in topologyDataTable.GetRows(selectedNodeType.AsIndexSet(), Array[DataValueCursor](topologyDataTableCur.values())):
        tableName = topologyDataTableCur['Topology Table'].CurrentValue
        keyName = topologyDataTableCur['Key'].CurrentValue
        print tableName

    return tableName + ',' + keyName

def runQuery(sql):
    connString = "DSN=" + Document.Properties['ENIQDataSourcesDropDown']
    connection = OdbcConnection(connString)

    dataSet = DataSet()
    connection.Open()
    adaptor = OdbcDataAdapter(sql, connection)
    dataSet = DataSet()
    adaptor.Fill(dataSet)
    connection.Close()
    return dataSet


def generate_text_data(dataSet):
    textData = ""
    overallTextData = ""
    currRow = ""

    for row in dataSet.Tables[0].Rows:
        currRow = []
        currRow.append(str(row[0]))
        currRow.append(str(row[1]))
        currRow.append(str(row[2]))

        yield "%s\r\n" % (';'.join(currRow))


def getTableName(nodeType):
    nodetypes = {}
    nodetypes['EPG'] = 'GGSN'
    nodetypes['SBG'] = 'IMSSBG'

    if nodeType in nodetypes.keys():
        nodeType = nodetypes[nodeType]

    sql = "SELECT top 1 typename FROM LOG_LOADSTATUS WHERE TYPENAME LIKE \
    'DC_E_"+nodeType+"%' AND STATUS = 'LOADED' AND MODIFIED >= DATE(NOW()-7) ORDER \
    BY ROWCOUNT DESC, MODIFIED DESC"
    tableName=""
    try:
        conn_string = "DSN=" + dataSourceName
        connection = OdbcConnection(conn_string)
        connection.Open()

        command = connection.CreateCommand()
        command.CommandText = sql
        reader = command.ExecuteReader()
        loopguard = 0

        while reader.Read() and loopguard != 1:
            tableName = reader[0]
            loopguard = 1
        connection.Close()
        if "DC_E_ERBSG2" in tableName:
            tableName = tableName.replace("DC_E_ERBSG2", "DC_E_ERBS", 1)

        Document.Properties["ConnectionError"] = ""
        Document.Properties["NoTableError"] = ""

    except TypeError:
        tableName=""
        Document.Properties["NoTableError"] = "Error: Table does not exist in ENIQ. Please install relevent Tech Pack."
    except EnvironmentError:
        Document.Properties["ConnectionError"] = "Please check ENIQ DB Connection"

    return tableName


def getElementName(nodeType):

    elementMappingCur = createCursor(elementMappingTable)
    element=""
    table=getTableName(nodeType)

    elementName = elementMappingCur["ELEMENT"]
    tableName = elementMappingCur["TABLENAME"]
    nodeTypeEM = elementMappingCur["Node Type"]
    rowCount = elementMappingTable.RowCount

    rowsToInclude = IndexSet(rowCount, True)

    for row in elementMappingTable.GetRows(rowsToInclude, elementName, tableName, nodeTypeEM):
        if table == tableName.CurrentValue.replace("_RAW",""):
            element = ((elementName.CurrentValue).split('.'))[1]
    return element


def fetchDataFromENIQAsync_wildcard():
    try:
        dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
        ps.CurrentProgress.ExecuteSubtask(progressText)
        ps.CurrentProgress.CheckCancel()
        databaseDataSource = DatabaseDataSource(dataSourceSettings)
        if not Document.Data.Tables.Contains(wildcardCollectionTableName):  # If it does not exist, create new
            Document.Data.Tables.Add(wildcardCollectionTableName, databaseDataSource)
        else:  # If it exists, update it
            wildcardCollectionTable = Document.Data.Tables[wildcardCollectionTableName]
            wildcardCollectionTable.ReplaceData(databaseDataSource)
        Document.Properties[queryResult] = 'Connection OK'
        if Document.Properties['NodeConfiguration'] == "EDIT":
            Document.Properties['ConnectionError'] = '' 
            Document.Properties['SystemAreaErrorMsg'] = 'Collection Name, System Area, Node Type and, Eniq Data Source will be uneditable'
        else:
            Document.Properties['ConnectionError'] = ''
        #Document.Properties['ConnectionError'] = ''
    except ProgressCanceledException as pce:  # user cancelled
        print("ProgressCanceledException: ", pce)
        Document.Properties[queryResult] = 'User cancelled'
    except Exception as e:
        print("Exception: ", e)
        Document.Properties[queryResult] = 'Failed with exception : %s' % str(e)
        if Document.Properties['NodeConfiguration'] == "EDIT":
            Document.Properties['ConnectionError'] = 'Failed to fetch Nodes...Invalid wildcard expression!' 
            Document.Properties['SystemAreaErrorMsg'] = ''
        else:
            Document.Properties['ConnectionError'] = 'Failed to fetch Nodes...Invalid wildcard expression!' 
            
def remove_selectedNode():
    #This is to empty selected nodes
    dataTable2 = Document.Data.Tables["SelectedNodes"]
    dataTable2.RemoveRows(RowSelection(IndexSet(dataTable2.RowCount,True)))
            
def fetchDataFromENIQAsync(nodeType):

    try:
        print Document.Properties["SystemArea"]
        if (Document.Properties["SystemArea"] != "" and Document.Properties["NodeType"] != "") and (Document.Properties["SystemArea"] != None and Document.Properties["NodeType"] != None) and (Document.Properties["SystemArea"] != "None" and Document.Properties["NodeType"] != "None"):
            try:                
                topologyFields = getTopologyTableData(nodeTypeOriginal, systemArea).split(',')
                topologyTable = topologyFields[0]
                topologyKeyField  = topologyFields[1]

                sqlTopology = u"SELECT DISTINCT " + topologyKeyField + "  AS node, '" + nodeTypeOriginal + "' AS NodeType, '" + systemArea + "' as SystemArea FROM " + topologyTable + " WHERE STATUS = 'Active' ORDER BY " + topologyKeyField + " ASC"
                dataSet = runQuery(sqlTopology)
                Document.Properties["ConnectionError"] = ""
            except:
                print 'No topology table, using log loadstatus.'
                sqlLogLoad = u"SELECT DISTINCT " + getElementName(nodeType) + " AS node, '" + nodeTypeOriginal + "' AS NodeType, '" + systemArea + "' as SystemArea FROM " + getTableName(nodeType) + "_RAW"
                dataSet = runQuery(sqlLogLoad)

            tableName = 'NodeList'
            overallTextData = generate_text_data(dataSet)
            createTable(tableName, overallTextData)
            Document.Properties["NoTableError"] = ""
            Document.Properties["SystemAreaErrorMsg"] = ""
        else:
            Document.Properties["SystemAreaErrorMsg"] = "Please select a System Area/Node Type/ENIQ Data Source"
    except:
        print "except"

def addSelectedNodes(table,textData):
    if Document.Data.Tables['SelectedNodes'].RowCount!= 0:
        SelectedNodeTable = Document.Data.Tables['SelectedNodes']
        SelectedNodeTable.RemoveRows(RowSelection(IndexSet(SelectedNodeTable.RowCount,True)))                                                                                                                                                                                               
    if textData != "CollectionName\tNodeName\tNodeType\tSystemArea\tCollectionType\tEniqDs\r\n":
        stream = MemoryStream()
        writer = StreamWriter(stream)
        writer.Write(textData)
        writer.Flush()
        stream.Seek(0, SeekOrigin.Begin)
        readerSettings = TextDataReaderSettings()
        readerSettings.Separator = "\t"
        readerSettings.AddColumnNameRow(0)
        readerSettings.SetDataType(0, DataType.String)
        readerSettings.SetDataType(1, DataType.String)
        readerSettings.SetDataType(2, DataType.String)
        dSource = TextFileDataSource(stream, readerSettings)
        settings = AddRowsSettings(table,dSource)
        table.AddRows(dSource, settings)


def showInSelectedNodesTable(table):
    fromTable = Document.Data.Tables['Wildcard Collections']
    fromCursor = createCursor(fromTable)
    nodeList = []
    #dt = dataTable.NewRow()
    textData = "CollectionName\tNodeName\tNodeType\tSystemArea\tCollectionType\tEniqDs\r\n"
    from_filter = fromTable.Select('CollectionName ="' + collectionName + '"')
    for row in fromTable.GetRows(from_filter.AsIndexSet(),Array[DataValueCursor](fromCursor.values())):
        textData += fromCursor['CollectionName'].CurrentValue + "\t" + fromCursor['NodeName'].CurrentValue + "\t" + fromCursor['NodeType'].CurrentValue + "\t" + fromCursor['SystemArea'].CurrentValue + "\t" + 'Dynamic Collection' + "\t" + dataSourceName +  "\r\n"
        
    addSelectedNodes(table,textData)

def remove_rows(collectionName):
    nodeCollectionTable = Document.Data.Tables['NodeCollection']
    nodeCollectionTableCur = createCursor(nodeCollectionTable)
    selectedNodeCollection = nodeCollectionTable.Select("[CollectionName]= '" + collectionName + "'")
    for row in nodeCollectionTable.GetRows(selectedNodeCollection.AsIndexSet(), Array[DataValueCursor](nodeCollectionTableCur.values())):
        if not nodeCollectionTableCur['CollectionName'].CurrentValue:
            SelectedNodeTable = Document.Data.Tables['SelectedNodes']
            SelectedNodeTable.RemoveRows(RowSelection(IndexSet(SelectedNodeTable.RowCount,True)))

def update_error_msg():
    system_area = Document.Properties["SystemArea"]
    node_type = Document.Properties["NodeType"]
    if system_area == None or node_type == None:
        Document.Properties["FetchErrorMsg"] = "Please select a valid System Area/ Node Type"
    else: 
        Document.Properties["FetchErrorMsg"] = ""
    


ps = Application.GetService[ProgressService]()
nodeType = Document.Properties["NodeType"]
nodeTypeOriginal = Document.Properties["NodeType"]
nodeType = nodeType.replace("-","")
systemArea = Document.Properties["SystemArea"]
dataSourceName = Document.Properties["ENIQDataSourcesDropDown"]
elementMappingTable = Document.Data.Tables["Measure Mapping"]
sql = ""
collectionName = Document.Properties["CollectionName"]
#wildcardCollectionName = Document.Properties["WildcardCollectionName"]
wildcardCollectionTableName = 'Wildcard Collections'
queryResult = "DwhdbConnectionResult"  # save result of query in this document property
progressText = 'Refreshing nodes from %s ...' % dataSourceName


if Document.ActivePageReference.Title == 'Node Collection  Manager':
    if Document.Properties['Dynamiccollection'] == 'ON':
        wildcardExpression = Document.Properties["WildcardExpression"]
                            
        if (nodeTypeOriginal != "" and systemArea != "" and wildcardExpression != "") and(nodeTypeOriginal != None and systemArea != None and wildcardExpression != None) and (nodeTypeOriginal != 'None' and systemArea != 'None' and wildcardExpression != 'None'):
            sql = CreateSQL(nodeTypeOriginal,systemArea,collectionName,wildcardExpression,dataSourceName)
            fetchDataFromENIQAsync_wildcard()
            Document.Properties["SystemAreaErrorMsg"] = ""
        else:
            Document.Properties["SystemAreaErrorMsg"] = "Please select a System Area/Node Type/ENIQ Data Source/WildCardExpression"
        selectedNodesTable = Document.Data.Tables['SelectedNodes']   
        textData = "CollectionName\tNodeName\tNodeType\tSystemArea\tCollectionType\tEniqDs\r\n"                                      
        showInSelectedNodesTable(selectedNodesTable)
        #ps.ExecuteWithProgress(progressText, progressText, fetchDataFromENIQAsync)
    else:                                                 
        print "nodeType: ", nodeType
        fetchDataFromENIQAsync(nodeType)
        remove_selectedNode()
else:
    fetchDataFromENIQAsync(nodeType)
remove_rows(collectionName)
update_error_msg()