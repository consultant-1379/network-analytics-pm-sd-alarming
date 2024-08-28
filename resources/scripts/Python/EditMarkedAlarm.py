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
# Name    : EditMarkedAlarm.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from System import Array
from Spotfire.Dxp.Data import DataValueCursor, IndexSet, DataType
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from collections import OrderedDict
from Spotfire.Dxp.Application.Filters import ListBoxFilter
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)


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


conn_string = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))


class AlarmState:
    Active = 'Active'
    Inactive = 'Inactive'


class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'MeasureName'
    Condition = 'Condition'
    ThresholdValue = 'ThresholdValue'
    Severity = 'Severity'  # AlarmLevel
    AlarmState = 'AlarmState'
    NECollection = 'NECollection'  # NeList
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'
    Schedule = 'Schedule'
    Aggregation = 'Aggregation'
    MeasureType = 'MeasureType'
    SingleOrCollection = 'SingleOrCollection'
    SingleNodeValue = 'SingleNodeValue'


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


def inValidForENM(*args):
    """checking for values that are unacceptable characters in ENM 
    **kwargs --> takes inputs a key value pair:
    """
    response = True
    if len(args) != 0:
        for value in args:
            if '#' in value or '?' in value:
                response = False
    return response


def validateEmptyFeilds(od):
    """
    checking for values if it contains any symbols which is not accepted by ENM
    """
    response = ''
    print type(od)
    if len(od) != 0:
        for key, value in od.items():
            print key, value
            if value != None: 
                if not value.strip():
                    response = key
                    break
            else:
                response = "Required Feild cannot be None"
    return response



def resetValues():
    Document.Properties['AlarmName'] = ''
    Document.Properties['AlarmType'] = 'Threshold'
    Document.Properties['NOCollection'] = ''
    Document.Properties['KPIType'] = 'Counter'
    Document.Properties['Condition'] = '<='
    Document.Properties['Severity'] = 'MINOR'
    Document.Properties['Schedule'] = '15'
    Document.Properties['ProbableCause'] = ''
    Document.Properties['SpecificProblem'] = ''
    srcTable = Document.Data.Tables["Measure Mapping"]
    filt=Document.FilteringSchemes[0][srcTable][srcTable.Columns["Measure"]].As[ListBoxFilter]()
    filt.Reset()

def createCursor(eTable):
    """Create cursors for a given table, these are used to loop through columns

    Arguments:
        eTable {data table} -- table object

    Returns:
        cursDict -- dictionary of cursors for the given table
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


# function



def changeView():

    tableName = 'NodeList'
    stream = MemoryStream()
    csvWriter = StreamWriter(stream)
    csvWriter.WriteLine("node;NodeType\r\n")
    csvWriter.Flush()
    createTable(tableName, stream)

    for page in Document.Pages:
	    if (page.Title == 'Alarm Definitions'):
		    Document.ActivePageReference=page


def updateAlarmDefinitionInDB(alarmDefinition):
    del alarmDefinition[11] #! temp workaround to get script working
    sql = ''
    sqlTemplate = '''
UPDATE [dbo].[tblAlarmDefinitions]
   SET [AlarmType] = '%s'
      ,[MeasureName] = '%s'
      ,[Condition] = '%s'
      ,[ThresholdValue] = '%s'
      ,[Severity] = '%s'
      ,[SpecificProblem] = '%s'
      ,[ProbableCause] = '%s'
      ,[Schedule] = '%s'
      ,[Aggregation] = '%s'
      ,[MeasureType] = '%s'
       ,[AlarmState] = 'Inactive'
       ,[CollectionID] = %s
 WHERE [AlarmName] = '%s'
'''
    if Document.Properties[AlarmColumn.SingleOrCollection] == 'Single Node':

        selectedNodeDataTableName = "NodeList"
        selectedNodeDataTable = Document.Data.Tables[selectedNodeDataTableName]
        selectedNodeCur = createCursor(selectedNodeDataTable)

        nodeType = getDistinctValuesList(selectedNodeDataTable, selectedNodeCur, 'NodeType')
        systemArea = Document.Properties['SystemArea']

        collectionName = Document.Properties['SingleNodeValue']
        collectionType = 'Single Node'
        
        sqlInsertCollectionTemplate = "INSERT INTO tblCollection (collectionName, nodeType, systemArea, collectionType)" \
            "VALUES ('"+ collectionName  \
                + "','" \
                + nodeType[0] \
                + "','" \
                + systemArea \
                + "','" \
                + collectionType \
                +"')"
        sql += sqlInsertCollectionTemplate + " "

        #if adding a single node collection then need to replace the collection id col in alarm def with
        #id created in collection table
        alarmDefinition[-1] = '(SELECT SCOPE_IDENTITY())'
    else:
        #if collection then just get the collection id based on collection name
        alarmDefinition[-1] = "(Select CollectionID from tblCollection where CollectionName = '"+alarmDefinition[-1]+"')"
    print(alarmDefinition)
    print('Alarm Name = ', alarmDefinition[0])
    print('Alarm items = ', alarmDefinition[1:])
    print tuple(alarmDefinition[1:] + alarmDefinition[:1])
    sql += sqlTemplate % tuple(alarmDefinition[1:] + alarmDefinition[:1])  # Alarm Name is at the start of the list, but at the end of the query, so need to shuffle
    print('sql = %s' % sql)

    dataSourceSettings = DatabaseDataSourceSettings("System.Data.SqlClient",
                                                     conn_string, sql)
    tableName = 'temp'
    databaseDataSource = DatabaseDataSource(dataSourceSettings)
    newDataTable = Document.Data.Tables.Add(tableName, databaseDataSource)
    Document.Data.Tables.Remove(tableName)


def ValidateErrors():
    isValid = True
    errorMessage = ""

    dp = OrderedDict()
    dp["Alarm Name"] = Document.Properties['AlarmName']
    dp["Alarm Type"] = Document.Properties['AlarmType']
    dp["Measure Input"] = Document.Properties['SelectedMeasureName']
    dp["Measure Type"] = Document.Properties['MeasureType']
    dp["Threshold ValueInput"] = str(Document.Properties['ThresholdValue'])
    dp["Probable Cause Input"] = Document.Properties['ProbableCause']
    dp["Specific Problem Input"] = Document.Properties['SpecificProblem']

    # check if single node or collection selected, and then check to see if values populated
    if Document.Properties[AlarmColumn.SingleOrCollection] == 'Single Node':
        dp['Single Node'] = Document.Properties[AlarmColumn.SingleNodeValue]

        #check if nodeType,systemarea and measure are all valid combinations for single node
        measureTablesDataTable = Document.Data.Tables['Measure Mapping']
        measureTableCur = createCursor(measureTablesDataTable)
        nodeType = Document.Properties['NodeType']
       
        systemArea = Document.Properties['SystemArea']
        selectedMeasure = measureTablesDataTable.Select("[Measure]= '" + dp["Measure Input"] + "'")
   
        if dp["Measure Input"] != '':
            for kpi in measureTablesDataTable.GetRows(selectedMeasure.AsIndexSet(), Array[DataValueCursor](measureTableCur.values())):
                currNodeType = measureTableCur['Node Type'].CurrentValue
                print currNodeType
                currSystemArea = measureTableCur['System Area'].CurrentValue
        
            if (currNodeType != nodeType) or (currSystemArea!= systemArea):
                errorMessage  += "Please ensure system area and node type are correct."
                isValid = False
    elif Document.Properties[AlarmColumn.SingleOrCollection] == 'Collection':
        dp["NE Collection"] = Document.Properties[AlarmColumn.NECollection]

    

    EmptyFields = validateEmptyFeilds(dp)
    ENMValid = inValidForENM(dp["Alarm Name"], dp["Probable Cause Input"], dp["Specific Problem Input"])

    if len(EmptyFields)>0 :
        isValid = False
        errorMessage = " please provide Value for: " + str(EmptyFields)
    if ENMValid == False:
        isValid = False
        errorMessage  += ", please remove '#' or '?' from the input fields"

    Document.Properties['ValidationError'] = errorMessage
    print errorMessage

    return isValid


AlarmState = AlarmState()  # Create an enum to represent alarm states
AlarmColumn = AlarmColumn()  # Create an enum to represent column names
alarmColumns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName, AlarmColumn.Condition, AlarmColumn.ThresholdValue,
                AlarmColumn.Severity, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause,
                AlarmColumn.Schedule, AlarmColumn.Aggregation, AlarmColumn.MeasureType, AlarmColumn.AlarmState, AlarmColumn.NECollection]
alarmDefinitionErrorPropertyName = 'AlarmDefinitionError'
Document.Properties[alarmDefinitionErrorPropertyName] = ''


alarmStateActive = 'Active'
alarmStateInactive = 'Inactive'
alarmDefinitionsDataTableName = 'Alarm Definitions'
dataTable = Document.Data.Tables[alarmDefinitionsDataTableName]
markedRowSelection = Document.ActiveMarkingSelectionReference.GetSelection(dataTable).AsIndexSet()



print (markedRowSelection.Count), "Hello"
cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in alarmColumns}

if ValidateErrors():
    Document.Properties[alarmDefinitionErrorPropertyName] = ''  # clear any existing warning
    updateAlarmDefinitionInDB(alarmDefinition=[Document.Properties['SelectedMeasureName'] if propertyName == 'MeasureName' else Document.Properties[propertyName.replace('_', '')] for propertyName in alarmColumns])
    resetValues()
    changeView()
   
         
Document.Data.Tables[alarmDefinitionsDataTableName].Refresh()


#for propertyName in alarmColumns:
 #  print Document.Properties[propertyName]

