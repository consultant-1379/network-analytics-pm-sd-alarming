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
import re
import os
from Spotfire.Dxp.Data import *
from System import Array
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcDataAdapter
from System import Environment, Threading
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System.Data.Odbc import OdbcConnection, OdbcCommand
clr.AddReference("System.Windows.Forms")
from System.Windows.Forms import OpenFileDialog, MessageBox, DialogResult, MessageBoxButtons
from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from Spotfire.Dxp.Application.Visuals import TablePlot, VisualTypeIdentifiers, LineChart, CrossTablePlot, HtmlTextArea
import ast
import re


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
#username = Threading.Thread.CurrentPrincipal.Identity.Name


_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)

notify = Application.GetService[NotificationService]()

def remove_selectedNode():
    #This is to empty selected nodes
    dataTable2 = Document.Data.Tables["SelectedNodes"]
    dataTable2.RemoveRows(RowSelection(IndexSet(dataTable2.RowCount,True)))

Document.Properties['ActionMessage'] = ""
def loadData():
    """Loads the selected KPI input in a data table."""
    OpenFile = OpenFileDialog()
    OpenFile.ShowDialog()
    settings= TextDataReaderSettings()
    settings.Separator = ","
    settings.AddColumnNameRow(0)
    filename = OpenFile.FileName
    filePath=filename
    if filePath != "":
        f = open(filePath, "r")
    
        NodeNameList = []
    
        data = f.readlines()
        for x in data:
            if 'name' in x:
                collection_name = x.split(':')[1].strip().replace('"','')
                Document.Properties["CollectionName"] = collection_name
        
            if 'FDN' in x:
                nodeName = re.search('.*[ManagedElement|MeContext]=(.*)[^A-Z|a-z|0-9]', x)
                found = nodeName.group(1).strip()
                NodeNameList.append(found)
                Document.Properties["NodeNameList"] = str(NodeNameList)
                
            if 'userId' in x:
                username = x.split(':')[1].strip().replace('"','')
                CreatedBy =  username.strip()
                Document.Properties["CreatedBy"] = CreatedBy
            
        
        
        
            #if 'NodeType' in x:
                #nodeType = x.split(':')[1].strip()
                #print('nodeType: '+str(nodeName))

            ENIQName = Document.Properties["ENIQDB"]
            dataTable2 = Document.Data.Tables["SelectedNodes"]
            dataTable2.RemoveRows(RowSelection(IndexSet(dataTable2.RowCount,True)))
            CreatedOn = str(date.today()).strip()
            Document.Properties["CreatedOn"] = CreatedOn
        return collection_name, NodeNameList, CreatedBy, CreatedOn
    else:
        Document.Properties['ActionMessage'] = "No file selected, please try again!"
        remove_selectedNode()
        return ('','','','')


collection_name, NodeNameList, CreatedBy, CreatedOn = loadData()

#for row in node_list_data_table.GetRows(filteredRows,myColCursor):
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


NodeCollectionColumn = NodeCollectionColumn()

#NodeTypeDP = Document.Properties['NodeType']
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
#dataTable.Columns.Add(NodeCollectionColumn.NodeType, System.String)
#dataTable.Columns.Add(NodeCollectionColumn.SystemArea, System.String)
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

for row in NodeNameList:
    # cursorValue will now contain the value for the column at the current row position
    node = row
    if node not in selected_nodes_list:
        '''if selected_node_table.RowCount == 0 or verifyNodeType(getNodeType(selectedNodeDataTableName), NodeTypeDP) == True and eniqDsSelected() != None:'''
        
        dt = dataTable.NewRow()
        dt[NodeCollectionColumn.CollectionName] = CollectionName
        dt[NodeCollectionColumn.NodeName] = node
        #dt[NodeCollectionColumn.NodeType] = NodeTypeVar
        #dt[NodeCollectionColumn.SystemArea] = SystemAreaVar
        dt[NodeCollectionColumn.CollectionType] = collectionType
        #dt[NodeCollectionColumn.EniqDs] = Document.Properties['ENIQDB']
        dataTable.Rows.Add(dt)
        Document.Properties['ConnectionError'] = ""
        

textData = "CollectionName\tNodeName\tNodeType\tSystemArea\tCollectionType\tEniqDs\r\n"
for row in dataTable.Rows:
    textData += row[NodeCollectionColumn.CollectionName] + "\t" + row[NodeCollectionColumn.NodeName] + "\t"  + row[NodeCollectionColumn.CollectionType] +  "\r\n"

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
        #settings_nodelist = AddRowsSettings(Document.Data.Tables["NodeList"],dSource)
        if Document.Properties['NodeType'] != "" and Document.Properties['SystemArea'] != "" :
            Document.Data.Tables["SelectedNodes"].AddRows(dSource, settings)
        else:
            Document.Properties['ActionMessage'] = "Select Node Type and System Area"
        
        #Document.Data.Tables["NodeList"].AddRows(dSource, settings_nodelist)

        
tableName = 'NodeList'
nodeNameFormated = []       
for node in NodeNameList:
    nodeNameFormated.append(node + ";MGW;"+ "Core")
#createTable(tableName, overallTextData)


#def createTable(dataTableName, textData):
stream = MemoryStream()
writer = StreamWriter(stream)
writer.WriteLine("node;NodeType;SystemArea\r\n")
writer.Flush()

for line in nodeNameFormated:
    writer.WriteLine(line)
writer.Flush()
settings = TextDataReaderSettings()
settings.Separator = ";"
settings.AddColumnNameRow(0)
stream.Seek(0, SeekOrigin.Begin)
fs = TextFileDataSource(stream, settings)

if Document.Data.Tables.Contains(tableName):
    dataTable = Document.Data.Tables[tableName]
    # clear table

    dataTable.RemoveRows(RowSelection(IndexSet(dataTable.RowCount,True)))
    settings = AddRowsSettings(dataTable, fs)    
    if Document.Properties['NodeType'] != "" and Document.Properties['SystemArea'] != "" :
        dataTable.AddRows(fs, settings)
    else:
        Document.Properties['ActionMessage'] = "Select Node Type and System Area"
else:
    # Create Table if not already present
    if Document.Properties['NodeType'] != "" and Document.Properties['SystemArea'] != "" :
        dataTable = Document.Data.Tables.Add(dataTableName, fs)
    else:
        Document.Properties['ActionMessage'] = "Select Node Type and System Area"

for page in Document.Pages:
  if page.Title == "Node Collection  Manager":
     Document.ActivePageReference = page

for page in Application.Document.Pages:
    if Document.ActivePageReference == page:
        for vis in page.Visuals:
            if vis.TypeId == VisualTypeIdentifiers.HtmlTextArea and vis.Title == 'Text Area':
                source_html = vis.As[HtmlTextArea]().HtmlContent
                deshtml=source_html     
                if Document.Properties['Dynamiccollection'] == 'ON':   
                    deshtml = re.sub('<DIV style="VISIBILITY: (visible|hidden)"><SPAN id=addbutton><SpotfireControl id="46c421ec1f0d441b87c10f7a17beb8ee" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV style="VISIBILITY: hidden"><SPAN id=addbutton><SpotfireControl id="46c421ec1f0d441b87c10f7a17beb8ee" /></SPAN></DIV> <DIV style="VISIBILITY: visible">',deshtml)                    
                    deshtml = re.sub('<DIV align=center style="VISIBILITY: (visible|hidden)"><SPAN id=removebutton><SpotfireControl id="fdd0a79046ee4cfaa4f29dd9dbd60c5a" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV align=center style="VISIBILITY: hidden"><SPAN id=removebutton><SpotfireControl id="fdd0a79046ee4cfaa4f29dd9dbd60c5a" /></SPAN></DIV> <DIV style="VISIBILITY: visible">',deshtml)
                else:   
                    if  Document.Data.Tables['NodeList'].RowCount == 0:                     
                        deshtml = re.sub('<DIV style="VISIBILITY: (visible|hidden)"><SPAN id=addbutton><SpotfireControl id="46c421ec1f0d441b87c10f7a17beb8ee" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV style="VISIBILITY: hidden"><SPAN id=addbutton><SpotfireControl id="46c421ec1f0d441b87c10f7a17beb8ee" /></SPAN></DIV> <DIV style="VISIBILITY: visible">',deshtml)
                    else:
                        deshtml = re.sub('<DIV style="VISIBILITY: (visible|hidden)"><SPAN id=addbutton><SpotfireControl id="46c421ec1f0d441b87c10f7a17beb8ee" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV style="VISIBILITY: visible"><SPAN id=addbutton><SpotfireControl id="46c421ec1f0d441b87c10f7a17beb8ee" /></SPAN></DIV> <DIV style="VISIBILITY: hidden">',deshtml)
                    if Document.Data.Tables['SelectedNodes'].RowCount == 0:    
                        deshtml = re.sub('<DIV align=center style="VISIBILITY: (visible|hidden)"><SPAN id=removebutton><SpotfireControl id="fdd0a79046ee4cfaa4f29dd9dbd60c5a" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV align=center style="VISIBILITY: hidden"><SPAN id=removebutton><SpotfireControl id="fdd0a79046ee4cfaa4f29dd9dbd60c5a" /></SPAN></DIV> <DIV style="VISIBILITY: visible">',deshtml)    
                    else:
                        deshtml = re.sub('<DIV align=center style="VISIBILITY: (visible|hidden)"><SPAN id=removebutton><SpotfireControl id="fdd0a79046ee4cfaa4f29dd9dbd60c5a" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV align=center style="VISIBILITY: visible"><SPAN id=removebutton><SpotfireControl id="fdd0a79046ee4cfaa4f29dd9dbd60c5a" /></SPAN></DIV> <DIV style="VISIBILITY: hidden">',deshtml)
                vis.As[HtmlTextArea]().HtmlContent = deshtml