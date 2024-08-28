# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2021 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : MeasureMappingENIQ.py
# Date    : 21/10/2021
# Revision: 4.0
# Purpose : Map ENIQ pm tables to Measures
#
# Usage   : PM Alarms

import Spotfire.Dxp.Application
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Framework.ApplicationModel import *
from System import Array, String
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Framework.Library import *
from Spotfire.Dxp.Data.Import import SbdfLibraryDataSource
import sys
import time


measureTableFinal = 'Measure Mapping'
Document.Data.Tables[measureTableFinal].RemoveRows(RowSelection(IndexSet(Document.Data.Tables[measureTableFinal].RowCount,True)))
Document.Data.Tables[measureTableFinal].Refresh()
Document.Properties['ErrorMessageServer']=''
list_of_notConnectedServers=[]

def createTable(dataTableName, stream):
    settings = TextDataReaderSettings()
    settings.Separator = ";"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)
    settings.SetDataType(0, DataType.String)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)
    if Document.Data.Tables.Contains(dataTableName):
        Document.Data.Tables[dataTableName].ReplaceData(fs)
    else:
        Document.Data.Tables.Add(dataTableName, fs)



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



def connectedEniqList(datatable):
     #takes data table name as parameter and returns list of connected Eniq data sources
     eniqDataSourceNames = list()
     dataTableCursor = createCursor(datatable)
     for eniqRow in datatable.GetRows(Array[DataValueCursor](dataTableCursor.values())):
         eniqDataSourceNames.append(dataTableCursor['EniqName'].CurrentValue)
     return eniqDataSourceNames 


def fetchDataFromENIQAsync(dataSourceName, sql, tableName):
    flag=False
    dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
    try:
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        if Document.Data.Tables.Contains(tableName):
            dt = Document.Data.Tables[tableName]
            dt.ReplaceData(dataTableDataSource)
        else:
            dt = Document.Data.Tables.Add(tableName, dataTableDataSource)
        settings = DataTableSaveSettings(dt,False, True)
        Document.Data.SaveSettings.DataTableSettings.Add(settings)
    except Exception as e:
        flag = True
        print e.message
	return flag




def deleteInvalidRows(datatable):
     #function deletes the rows with empty eniq info, from past file
      
     EniqDataSourceColumn = datatable.Columns['DataSourceName']
     emptyValues=EniqDataSourceColumn.RowValues.InvalidRows
     if emptyValues.Count>0:
	     
         RowCount=datatable.RowCount
         rowsToFilter=IndexSet(RowCount,False)
         dataTableCursor = createCursor(datatable)
         for measureMappingRow in datatable.GetRows(Array[DataValueCursor](dataTableCursor.values())):
             if dataTableCursor['DataSourceName'].CurrentValue == '(Empty)':
                 rowsToFilter.AddIndex(measureMappingRow.Index)
         
         datatable.RemoveRows(RowSelection(rowsToFilter))
        


def getDataTable(tableName):
    try:
        return Document.Data.Tables[tableName]
    except:
        return None


if getDataTable(measureTableFinal) != None: 
    deleteInvalidRows(Document.Data.Tables[measureTableFinal])


measureColumns = '''Measure
Counters
Formula
Node Type
System Area
Measure Type
DESCRIPTION
LTE Access Type
WCDMA Access Type
GSM Access Type
Access Type
NR Access Type
MOCLASS
Complexity
TABLENAME
KEYS
ELEMENT
TIMEAGGREGATION
GROUPAGGREGATION
SELECT OPERATION
WHERE CLAUSE
GROUP BY
CUSTOM KEYS
SQL OPERATOR
Mapping Columns
Category
COLLECTIONMETHOD
DataSourceName'''.split('\n')

topologyColumns = '''Topology Table
Node
System Area
Key
DataSourceName
FDN Key'''.split('\n')

eniqColumns = '''COUNTER
DataSourceName'''.split('\n')



connectedEniqs = 0
eniqDataSources = Document.Data.Tables["EniqEnmMapping"]
connectedEniqs = connectedEniqList(eniqDataSources)
connectedEniqs = set(connectedEniqs)



if len(connectedEniqs) == 0:
    sys.exit("No Connected Eniq Information Found")




def fetchingDataFor():
    # fucntion allows some time for progress bar to be shown 
    time.sleep(6)    



ps = Application.GetService[ProgressService]()        
ps.ExecuteWithProgress("Fetching counter and mapping data", "Please be patient, fetching data from ENIQ can take some time", fetchingDataFor)
#fetchingDataFor()

eniqCountersTable = 'ENIQ Counters'
topologyDataTable = 'Topology Data'
	

if Document.Data.Tables.Contains(eniqCountersTable):
	Document.Data.Tables.Remove(eniqCountersTable)


if Document.Data.Tables.Contains(topologyDataTable):
	Document.Data.Tables.Remove(topologyDataTable)

stream = MemoryStream()
csvWriter = StreamWriter(stream)
csvWriter.WriteLine(';'.join(eniqColumns) + '\r\n')
csvWriter.Flush()
createTable(eniqCountersTable, stream)


topologyStream = MemoryStream()
topologyCsvWriter = StreamWriter(topologyStream)
topologyCsvWriter.WriteLine(';'.join(topologyColumns) + '\r\n')
topologyCsvWriter.Flush()
createTable(topologyDataTable, topologyStream)

def execute():
	measures = []
	count = 0
	for eniq in connectedEniqs:
		
		count += 1
		dataSourceName = ''
		DSName = eniq
		ps = Application.GetService[ProgressService]()

		sql = """
		SELECT
		DISTINCT UPPER(DATANAME) AS COUNTER, '{DSName}' as DataSourceName
		FROM DWHREP.MEASUREMENTCOUNTER MC
		INNER JOIN TPACTIVATION TP
		ON MC.TYPEID LIKE TP.VERSIONID + '%'
		WHERE TP.STATUS = 'ACTIVE'
		""".format(DSName = DSName)
		
		tableName = eniqCountersTable
		dataSourceName = eniq + 'repdb'
		
		
		Temp_flag = fetchDataFromENIQAsync(dataSourceName, sql, tableName)
		
		MeasureDataTableName = 'Measures'
		MeasureDataTable = Document.Data.Tables[MeasureDataTableName]
		measureCursor = createCursor(MeasureDataTable)
		
		ENIQDataTableName = eniqCountersTable
		ENIQDataTable = Document.Data.Tables[ENIQDataTableName]
		ENIQCursor = createCursor(ENIQDataTable)

		
 
		for measureRow in MeasureDataTable.GetRows(Array[DataValueCursor](measureCursor.values())):
			counterListCheck = measureCursor['Counters'].CurrentValue.upper().replace('"', '').split(',')
		 
			category = measureCursor['Category'].CurrentValue.replace(' ','')
			kpi_name = measureCursor['Measure'].CurrentValue
		
			counterList = []
			for i in counterListCheck:
				if '.' in i:
					counterList.append(i.split('.')[1].upper())
				else:
					counterList.append(i.upper())
		
			query = ' OR '.join("[COUNTER]='{0}'".format(c.upper()) for c in counterList)
			indexSet = ''
			indexFilter = Document.Data.Tables[ENIQDataTableName].Select(query)
			indexSet = indexFilter.AsIndexSet()
			eniqTableList = []
			row = []
		   
			for col in measureColumns:
				if col == 'DataSourceName':
					row.append(DSName)
				else:
					row.append(measureCursor[col].CurrentValue)
		
			for eniqRow in ENIQDataTable.GetRows(indexSet, Array[DataValueCursor](ENIQCursor.values())):
				eniqTableList.append(ENIQCursor['COUNTER'].CurrentValue.upper())
		
			measureFound = True
		
			for measureCounter in counterList:
				if measureCounter not in eniqTableList:
					measureFound = False
					break
			
			if measureFound:
				measures.append(';'.join(row))
		
		if count == len(connectedEniqs):
			measureStream = MemoryStream()
			measureWriter = StreamWriter(measureStream)
			measureWriter.WriteLine(';'.join(measureColumns) + '\r\n')
			measureWriter.Write('\r\n'.join(measures))   
			measureWriter.Flush()
			measureStream.Seek(0, SeekOrigin.Begin)
			createTable(measureTableFinal, measureStream)
            
	topologyRows = []


	for eniq in connectedEniqs: 
		manager = Application.GetService[LibraryManager]()
		libraryPaths = ["/Ericsson Library/General/PM Data/PM-Data/Analysis/Custom Measure Mapping","/Ericsson Library/General/PM Data/PM-Data/Analysis/Service Measure Mapping"]
		for libraryPath in libraryPaths:
			(found, item) = manager.TryGetItem(libraryPath,LibraryItemType.SbdfDataFile,LibraryItemRetrievalOption.IncludePath)
			if found:
				ds = SbdfLibraryDataSource(item)
				rowsettings = AddRowsSettings(Document.Data.Tables[measureTableFinal],ds, "DataSourceName",eniq)
				Document.Data.Tables[measureTableFinal].AddRows(ds,rowsettings)
		
		libraryPathsTopology = ["/Ericsson Library/General/PM Data/PM-Data/Analysis/Topology Data","/Ericsson Library/General/PM Data/PM-Data/Analysis/Service Topology Data"]
		for libraryPath in libraryPathsTopology:
			(found, item) = manager.TryGetItem(libraryPath,LibraryItemType.SbdfDataFile,LibraryItemRetrievalOption.IncludePath)
			if found:
				ds = SbdfLibraryDataSource(item)
				rowsettings = AddRowsSettings(Document.Data.Tables[topologyDataTable],ds,"DataSourceName",eniq)
				Document.Data.Tables[topologyDataTable].AddRows(ds,rowsettings)
                
		sql = """SELECT
		DISTINCT Upper(TypeName) as TableName, '{DSName}' as DataSourceName
		FROM DWHREP.ReferenceTable RT
		INNER JOIN TPACTIVATION TP
		ON RT.TYPEID LIKE TP.VERSIONID + '%'
		WHERE TP.STATUS = 'ACTIVE' And TP.TYPE = 'Topology'""".format(DSName=eniq)
		
		tableName = 'temp_topology'
		dataSourceName = eniq + 'repdb'
		
		
		Temp_flag = fetchDataFromENIQAsync(dataSourceName, sql, tableName)
		if Temp_flag:
			list_of_notConnectedServers.append(eniq)
			continue
		topologyTable = Document.Data.Tables[tableName]
        
		temp_topologyCur = createCursor(topologyTable)
		TablsPresent = []
		for row in topologyTable.GetRows(Array[DataValueCursor](temp_topologyCur.values())):
			TablsPresent.append(temp_topologyCur['TableName'].CurrentValue)
            
		query = ' OR '.join("[Topology Table]='{0}'".format(c.upper()) for c in TablsPresent)
        
		query = '(' + query + ') AND [DataSourceName] = "' + eniq + '"'
        
		indexSet = ''
		indexFilter = Document.Data.Tables['Topology Data'].Select(query)
		indexSet = indexFilter.AsIndexSet()
		TopologyDataTable = Document.Data.Tables['Topology Data'] 
		topologyCursor = createCursor(TopologyDataTable)
        
		for row in TopologyDataTable.GetRows(indexSet,Array[DataValueCursor](topologyCursor.values())):
			rowtemp = []
			for col in topologyColumns:
				rowtemp.append(topologyCursor[col].CurrentValue)
			topologyRows.append(';'.join(rowtemp))
            
	
            
	ModifiedtopologyTable = 'Modified Topology Data'
            
	if Document.Data.Tables.Contains(ModifiedtopologyTable):
			Document.Data.Tables.Remove(ModifiedtopologyTable)
	if Document.Data.Tables.Contains('temp_topology'):
			Document.Data.Tables.Remove('temp_topology')
		
	topologyStream = MemoryStream()
	topologyCsvWriter = StreamWriter(topologyStream)
	topologyCsvWriter.WriteLine(';'.join(topologyColumns) + '\r\n')
	topologyCsvWriter.Write('\r\n'.join(topologyRows))
	topologyCsvWriter.Flush()
	topologyStream.Seek(0, SeekOrigin.Begin)
	createTable(ModifiedtopologyTable, topologyStream)
	string = ''
	if len(list_of_notConnectedServers)>0:
			#for item in list_of_notConnectedServers:
			string = ' , '.join(list_of_notConnectedServers) 
			Document.Properties['ErrorMessageServer'] = 'SyncWithEniq is unsuccessful for  ' + string +   '  Servers, please check the server connection.'
           
execute()
#print "list_of_notConnectedServers",list_of_notConnectedServers

