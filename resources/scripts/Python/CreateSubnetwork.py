from System.Collections.Generic import List
from Spotfire.Dxp.Data import DataColumn, DataColumnType
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Framework.ApplicationModel import ProgressService, ProgressCanceledException
from Spotfire.Dxp.Data import DataColumnSignature, DataFlowBuilder, AddRowsSettings
from Spotfire.Dxp.Data.Transformations import UnpivotTransformation, ExpressionTransformation, ChangeDataTypeTransformation
from Spotfire.Dxp.Data import *
from System import Array, String
from Spotfire.Dxp.Application.Visuals import *
from System.Drawing import Size
from System.Drawing import Color
from System.Drawing import *
from Spotfire.Dxp.Application.Visuals.ConditionalColoring import *
from Spotfire.Dxp.Application.Visuals import TablePlot, VisualTypeIdentifiers, LineChart, CrossTablePlot, HtmlTextArea
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
import re


SubnetworkMessage = 'SubnetworkMessage'
def fetchDataFromENIQAsync():
    try:
        ps.CurrentProgress.ExecuteSubtask(progressText)
        ps.CurrentProgress.CheckCancel()
        databaseDataSource = DatabaseDataSource(dataSourceSettings)
        if Document.Data.Tables.Contains(temp_table):      # If exists, replace it
            Document.Data.Tables[temp_table].ReplaceData(databaseDataSource)
        else:                                             # If it does not exist, create new
            Document.Data.Tables.Add(temp_table, databaseDataSource)
        Document.Properties[queryResult] = 'Connection OK'
        #Document.Properties[SubnetworkMessage] = 'Connection OK'
    except ProgressCanceledException as pce:  # user cancelled
        print("ProgressCanceledException: ", pce)
        Document.Properties[queryResult] = 'User cancelled'
    except Exception as e:
        print("Exception: ", e)
        Document.Properties[queryResult] = str(e.message)#'Failed with exception: %s' % str(e)

def deleteInvalidRows():
     #function deletes the rows with empty eniq info, from past file
     datatable = Document.Data.Tables['SubNetwork List'] 
     EniqDataSourceColumn = datatable.Columns['SubnetworkName']
     emptyValues=EniqDataSourceColumn.RowValues.InvalidRows
     if emptyValues.Count>0:
	     
         RowCount=datatable.RowCount
         rowsToFilter=IndexSet(RowCount,False)
         dataTableCursor = create_cursor(datatable)
         for measureMappingRow in datatable.GetRows(Array[DataValueCursor](dataTableCursor.values())):
             print dataTableCursor['SubnetworkName'].CurrentValue
             if dataTableCursor['SubnetworkName'].CurrentValue == '(Empty)':
                 rowsToFilter.AddIndex(measureMappingRow.Index)
         
         datatable.RemoveRows(RowSelection(rowsToFilter))


def removeCalculatedColumns(dataTable):
    columnCollection = dataTable.Columns
    columnsToRemove = List[DataColumn]()
    for column in columnCollection:
        # print('Column: ', column.Name, column.Properties.ColumnType)
        if column.Properties.ColumnType == DataColumnType.Calculated:
            # print('Column: ', column.Name, column.Properties.ColumnType)
            columnsToRemove.Add(column)
    columnCollection.Remove(columnsToRemove)


def buildSubNetworkCollections(subNetworkCollectionTable):
    removeCalculatedColumns(subNetworkCollectionTable)  # Remove any existing calculated columns first
    # subNetworkCollectionTable.Columns.AddCalculatedColumn('CreatedBy', "'SubNetwork'")
    # subNetworkCollectionTable.Columns.AddCalculatedColumn('CreatedOn', 'Date(DateTimeNow())')
    # subNetworkCollectionTable.Columns.AddCalculatedColumn('EniqName', "'{}'".format(dataSourceName))

    # Add in columns for each SubNetwork component of the FDN
    # This assumes a maximum of 4 SubNetworks, add more if necessary
    for i in range(1, 5):
        subNetworkCollectionTable.Columns.AddCalculatedColumn('SubNetwork_{}'.format(i), r"Substitute(RXExtract([FDN],'SubNetwork=[^,]+',{}),'SubNetwork=','')".format(i))
    # Remove any empty SubNetwork columns
    for column in subNetworkCollectionTable.Columns:
        if column.RowValues.InvalidRows.Count == subNetworkCollectionTable.RowCount:
            subNetworkCollectionTable.Columns.Remove(column)


def addSubNetworkCollections(subNetworkCollectionTable):
    dataSource = DataTableDataSource(subNetworkCollectionTable)
    dataSource.IsPromptingAllowed = False
    dataSource.ReuseSettingsWithoutPrompting = True
    dataFlowBuilder = DataFlowBuilder(dataSource, Application.ImportContext)

    # Add unpivot transformation to get SubNetwork names as one column
    unpivot = UnpivotTransformation()
    unpivot.ResultName = "SubnetworkName";
    unpivot.CategoryName = "SubnetworkNameSource";
    unpivot.IdentityColumns = List[DataColumnSignature]()
    unpivot.ValueColumns = List[DataColumnSignature]()
    for column in subNetworkCollectionTable.Columns:
        if 'SubNetwork' not in column.Name:
            unpivot.IdentityColumns.Add(DataColumnSignature(column))
        else:
            unpivot.ValueColumns.Add(DataColumnSignature(column))
    dataFlowBuilder.AddTransformation(unpivot)

    # Add a CollectionID column
    expressionTransformation = ExpressionTransformation()
    collectionIdOffset = 1000000
    expressionTransformation.ColumnAdditions.Add('SubnetworkID', '{} + Min(RowID( ) ) over([{}])'.format(collectionIdOffset, unpivot.ResultName))  # add a collection ID offset to avoid clash with user-defined IDs
    dataFlowBuilder.AddTransformation(expressionTransformation)

    # Create new table or replace data if existing table
    dataTableDataSource = dataFlowBuilder.Build()
    if Document.Data.Tables.Contains(nodeCollectionTableName):  # If exists, replace it
        Document.Data.Tables[nodeCollectionTableName].ReplaceData(dataTableDataSource)
    else:  # If it does not exist, create new
        Document.Data.Tables.Add(nodeCollectionTableName, dataTableDataSource)


def create_cursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""

    CursList = []
    ColList = []
    colname = []
    for eColumn in eTable.Columns:
        CursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        ColList.append(eTable.Columns[eColumn.Name].ToString())
    print ColList
    CursArray = Array[DataValueCursor](CursList)
    cusrDict = dict(zip(ColList, CursList))
    return cusrDict
    
def SqlCreation(server):   
    sql = '' 
    tableName = 'Modified Topology Data'
    MainTable = Document.Data.Tables[tableName]
    table_cursor = create_cursor(MainTable)
    #print table_cursor.values()
    for topology in MainTable.GetRows(Array[DataValueCursor](table_cursor.values())):
        tableCursor = table_cursor['Topology Table'].CurrentValue.upper()
        nodeCursor = table_cursor['Node'].CurrentValue
        systemCursor = table_cursor['System Area'].CurrentValue
        keyCursor = table_cursor['Key'].CurrentValue.upper()
        fdnCursor = table_cursor['FDN Key'].CurrentValue.upper()
        serverCursor = table_cursor['DataSourceName'].CurrentValue
        if serverCursor == server:
            if sql == '':
                sql = """
                SELECT DISTINCT            
                    {keyCursor}  as NodeName, 
                    {fdnCursor}  as FDN,  
                    '{nodeCursor}' as NodeType, 
                    '{systemCursor}' as SystemArea,
                    '{eniqS}' as DataSourceName
                FROM
                    {tableCursor}
                """.format(keyCursor=keyCursor,fdnCursor=fdnCursor,nodeCursor=nodeCursor,systemCursor=systemCursor,eniqS=server,tableCursor=tableCursor)
            else:
                sql = sql + """
                union all 
                SELECT DISTINCT            
                    {keyCursor}  as NodeName, 
                    {fdnCursor}  as FDN,  
                    '{nodeCursor}' as NodeType, 
                    '{systemCursor}' as SystemArea,
                    '{eniqS}' as DataSourceName
                FROM
                    {tableCursor}
                """.format(keyCursor=keyCursor,fdnCursor=fdnCursor,nodeCursor=nodeCursor,systemCursor=systemCursor,eniqS=server,tableCursor=tableCursor)
    print "sql"
    print sql
    return sql

def getAllConnectedServers():
    EniqTable = Document.Data.Tables['EniqEnmMapping']
    eniq_cursors = create_cursor(EniqTable)
    eniqSList = []
    for row in EniqTable.GetRows(Array[DataValueCursor](eniq_cursors.values())):
        eniqSList.append(eniq_cursors['EniqName'].CurrentValue)
    return eniqSList

def addCalCol():
    SubNetworkTable = Document.Data.Tables['SubNetwork List']
    col = SubNetworkTable.Columns
    col.AddCalculatedColumn('Filtersubnetwork','if ([NodeType] = "${NodeType}" and [SystemArea] = "${SystemArea}" and [DataSourceName] = "${ENIQDataSourcesDropDown}",[SubnetworkName])')
    col.AddCalculatedColumn('FilteredNodeType','if ([SystemArea] = "${SystemArea}" and [DataSourceName] = "${ENIQDataSourcesDropDown}",[NodeType])')
    
    
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

ps = Application.GetService[ProgressService]()
dataSourceName = Document.Properties["ENIQDB"]
subNetworkCollectionTableName = 'FDN SubNetwork List From ENIQ'
nodeCollectionTableName = 'SubNetwork List'
queryResult = "DwhdbConnectionResult"  # save result of query in this document property
temp_table = "temp_table"

subnetworktableColumns= """NodeName
FDN
NodeType
SystemArea
DataSourceName""".split('\n')


if Document.Data.Tables.Contains(subNetworkCollectionTableName):
    Document.Data.Tables.Remove(subNetworkCollectionTableName)

subnetworkErrorservers = ''   
subnetworkRows = []
flag = set()
for server in getAllConnectedServers():
    #print server
    sql = SqlCreation(server)
    #print sql
    dataSourceName = server
    progressText = 'Refreshing nodes from %s ...' % dataSourceName    
    dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
    #ps.ExecuteWithProgress(progressText, progressText, fetchDataFromENIQAsync)
    fetchDataFromENIQAsync()
    if Document.Properties[queryResult] == 'Connection OK':
		flag.add(False)
		#Document.Properties[SubnetworkMessage] = 'Successfully Connected!' 
		temp_data_table = Document.Data.Tables[temp_table]    
		temp_table_cursor = create_cursor(temp_data_table)
		for row in temp_data_table.GetRows(Array[DataValueCursor](temp_table_cursor.values())):
			rowtemp = []
			for col in subnetworktableColumns:
				rowtemp.append(temp_table_cursor[col].CurrentValue)
			subnetworkRows.append(';'.join(rowtemp))        
    elif Document.Properties[queryResult] == 'User cancelled':
		flag.add(True)
		Document.Properties[SubnetworkMessage] = 'User Has Stopped the Process'
		break  
    else:
		flag.add(True)
		subnetworkErrorservers = subnetworkErrorservers + ', ' + server
		'''if 'Unable to execute' in Document.Properties[queryResult]:
			Document.Properties[SubnetworkMessage] = 'Fetching of Subnetwork has failed!'
		else:
			Document.Properties[SubnetworkMessage] = Document.Properties[queryResult]
		break '''
        
subnetworktableStream = MemoryStream()
subnetworktableCsvWriter = StreamWriter(subnetworktableStream)
subnetworktableCsvWriter.WriteLine(';'.join(subnetworktableColumns) + '\r\n')
subnetworktableCsvWriter.Write('\r\n'.join(subnetworkRows)) 
subnetworktableCsvWriter.Flush()
subnetworktableStream.Seek(0, SeekOrigin.Begin)
createTable(subNetworkCollectionTableName, subnetworktableStream)
        

print "flag",flag
if Document.Data.Tables.Contains(temp_table):
    Document.Data.Tables.Remove(temp_table)
    
if True in flag and False in flag:	
    subNetworkCollectionTable = Document.Data.Tables[subNetworkCollectionTableName]
    buildSubNetworkCollections(subNetworkCollectionTable)
    addSubNetworkCollections(subNetworkCollectionTable)
    deleteInvalidRows()
    #addCalCol()
    subnetworkErrorservers = subnetworkErrorservers.strip(',')
    Document.Properties[SubnetworkMessage] = 'Error while fetching SubNetwork for ' + subnetworkErrorservers + '!'
elif False in flag:
    #print "End!"
    subNetworkCollectionTable = Document.Data.Tables[subNetworkCollectionTableName]
    buildSubNetworkCollections(subNetworkCollectionTable)
    addSubNetworkCollections(subNetworkCollectionTable)
    deleteInvalidRows()
    #addCalCol()
    Document.Properties[SubnetworkMessage] = 'SubNetwork fetched Successfully!'
    
else:
    Document.Properties[SubnetworkMessage] = 'Fetching of Subnetwork has failed!'

for page in Application.Document.Pages:
    if page.Title == 'Administration':
        for vis in page.Visuals:
            if vis.TypeId == VisualTypeIdentifiers.HtmlTextArea and vis.Title == 'Step 3: Fetch SubNetwork':
                #deshtml=resultsPage.Visuals.AddNew[HtmlTextArea]()
                source_html = vis.As[HtmlTextArea]().HtmlContent
                #deshtml.HtmlContent=source_html
                deshtml=source_html
                if not ('SubNetwork fetched Successfully!' in Document.Properties[SubnetworkMessage]):
                    deshtml = re.sub('<TD><FONT color=(#ff0000|#000000)><SpotfireControl id="9a9bea0d4bcd4bb9a52af8655cb824e5" /></FONT></TD>','<TD><FONT color=#ff0000><SpotfireControl id="9a9bea0d4bcd4bb9a52af8655cb824e5" /></FONT></TD>',deshtml)
                else:
                    deshtml = re.sub('<TD><FONT color=(#ff0000|#000000)><SpotfireControl id="9a9bea0d4bcd4bb9a52af8655cb824e5" /></FONT></TD>','<TD><FONT color=#000000><SpotfireControl id="9a9bea0d4bcd4bb9a52af8655cb824e5" /></FONT></TD>',deshtml)
                vis.As[HtmlTextArea]().HtmlContent = deshtml             
