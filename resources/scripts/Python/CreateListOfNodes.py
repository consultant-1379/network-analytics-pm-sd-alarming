# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2019 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : CreateListOfNodes.py
# Date    : 06/11/2019
# Revision: 1.0
# Purpose : Adds marked nodes from All Nodes to 
#			Selected Nodes
#
# Usage   : PM Explorer
#

import clr
clr.AddReference('System.Data')  # needs to be imported before System.Data is called
import System
from System.Data import DataSet, DataTable, XmlReadMode
from Spotfire.Dxp.Data import DataType, DataTableSaveSettings, AddRowsSettings
from System.IO import StringReader, StreamReader, StreamWriter, MemoryStream, SeekOrigin
from System.Threading import Thread
from Spotfire.Dxp.Data import IndexSet
from Spotfire.Dxp.Data import RowSelection
from Spotfire.Dxp.Data import DataValueCursor
from Spotfire.Dxp.Data import DataSelection
from Spotfire.Dxp.Data import DataPropertyClass
from Spotfire.Dxp.Data import Import
from System.Net import HttpWebRequest
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings

class NodeCollectionColumn:
    CollectionName = 'CollectionName'
    NodeName = 'NodeName'
    NodeType = 'NodeType'
    SystemArea = 'SystemArea'
    CollectionType = 'CollectionType'
    EniqDs = 'EniqDs'

def getSelectedNodesNames(dataTableName):
    selectedNodesNames = []
    selectedNodeType= []
    if Document.Data.Tables.Contains(dataTableName):
        dataTable = Document.Data.Tables[dataTableName]
        cursor = DataValueCursor.CreateFormatted(dataTable.Columns[NodeCollectionColumn.NodeName])
        rows = IndexSet(dataTable.RowCount, True)

        for row in dataTable.GetDistinctRows(rows, cursor):
            selectedNodesNames.append(cursor.CurrentValue)
            selectedNodeType.append(cursor.CurrentValue)
    return selectedNodesNames


def getNodeType(dataTableName):
    selectedNopdeType = ""
    if Document.Data.Tables.Contains(dataTableName):
        dataTable = Document.Data.Tables[dataTableName]
        cursor = DataValueCursor.CreateFormatted(dataTable.Columns[NodeCollectionColumn.NodeType])
        rows = IndexSet(dataTable.RowCount, True)
        for row in range(0, 1):
            selectedNodeType = dataTable.Columns['NodeType'].RowValues.GetFormattedValue(row)
    return selectedNodeType


def getSystemArea(dataTableName):
    selectedSystemArea = ""
    if Document.Data.Tables.Contains(dataTableName):
        dataTable = Document.Data.Tables[dataTableName]
        cursor = DataValueCursor.CreateFormatted(dataTable.Columns[NodeCollectionColumn.NodeType])
        rows = IndexSet(dataTable.RowCount, True)
        for row in range(0, 1):
            selectedSystemArea = dataTable.Columns['SystemArea'].RowValues.GetFormattedValue(row)
    return selectedSystemArea

def verifyNodeType(dataTableNodeType, NodeType):
    if dataTableNodeType == NodeType:
        return True
    else:
        return False


NodeCollectionColumn = NodeCollectionColumn()

NodeTypeDP = Document.Properties['NodeType']
collectionType = "Collection"  #default value for collections
CollectionName = Document.Properties["SelectedCollectionToModify"]
EniqDsName = ""

if CollectionName is None:
    CollectionName = Document.Properties['CollectionName']

selectedNodeDataTableName = 'SelectedNodes'
nodeCollectionsDataTableName = 'NodeCollection'
node_list_data_table = Document.Data.Tables['NodeList']
NodesInNodeList = DataValueCursor.CreateFormatted(node_list_data_table.Columns["SearchedNode"])
NodeTypeInNodeList = DataValueCursor.CreateFormatted(node_list_data_table.Columns["NodeType"])
selected_node_table = Document.Data.Tables[selectedNodeDataTableName]

selected_nodes_list = getSelectedNodesNames(selectedNodeDataTableName)

dataTable = DataTable("temp")
dataTable.Columns.Add(NodeCollectionColumn.CollectionName, System.String)
dataTable.Columns.Add(NodeCollectionColumn.NodeName, System.String)
dataTable.Columns.Add(NodeCollectionColumn.NodeType, System.String)
dataTable.Columns.Add(NodeCollectionColumn.SystemArea, System.String)
dataTable.Columns.Add(NodeCollectionColumn.CollectionType, System.String)
dataTable.Columns.Add(NodeCollectionColumn.EniqDs, System.String)

# Get a reference to the specified filtering scheme on the data table above
dataFilteringSelection = Document.Data.Filterings["Filtering scheme"]
filteringScheme = Document.FilteringSchemes[dataFilteringSelection]
filterCollection = filteringScheme[node_list_data_table]

# Filtered rows based on the scheme above
filteredRows = filterCollection.FilteredRows

# Specify the column in the data table to get the values
myColCursor = DataValueCursor.CreateFormatted(node_list_data_table.Columns["node"])


def eniqDsSelected():
    global EniqDsName
    if Document.Properties["AvailableENIQDataSources"] != "":
        return Document.Properties["AvailableENIQDataSources"]
    else:
        Document.Properties['ConnectionError'] = "Please select the ENIQ Data Source" 
        return None



# Iterate over the filtered rows
for row in node_list_data_table.GetRows(filteredRows,myColCursor):
    # cursorValue will now contain the value for the column at the current row position
    node = myColCursor.CurrentValue

    if node not in selected_nodes_list:
        if selected_node_table.RowCount == 0 or verifyNodeType(getNodeType(selectedNodeDataTableName), NodeTypeDP) == True and eniqDsSelected() != None:
            dt = dataTable.NewRow()
            dt[NodeCollectionColumn.CollectionName] = CollectionName
            dt[NodeCollectionColumn.NodeName] = node
            dt[NodeCollectionColumn.NodeType] = getNodeType("NodeList")
            dt[NodeCollectionColumn.SystemArea] = getSystemArea("NodeList")
            dt[NodeCollectionColumn.CollectionType] = collectionType
            dt[NodeCollectionColumn.EniqDs] = eniqDsSelected()
            dataTable.Rows.Add(dt)
            Document.Properties['ConnectionError'] = ""
        else:
            Document.Properties['ConnectionError'] = "Please select the same node type"

textData = "CollectionName\tNodeName\tNodeType\tSystemArea\tCollectionType\tEniqDs\r\n"
for row in dataTable.Rows:
    textData += row[NodeCollectionColumn.CollectionName] + "\t" + row[NodeCollectionColumn.NodeName] + "\t" + row[NodeCollectionColumn.NodeType] + "\t" + row[NodeCollectionColumn.SystemArea] + "\t" + row[NodeCollectionColumn.CollectionType] + "\t" + row[NodeCollectionColumn.EniqDs] + "\r\n"

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

    if Document.Data.Tables.Contains(selectedNodeDataTableName):
        settings = AddRowsSettings(Document.Data.Tables["SelectedNodes"],dSource)
        Document.Data.Tables["SelectedNodes"].AddRows(dSource, settings)
