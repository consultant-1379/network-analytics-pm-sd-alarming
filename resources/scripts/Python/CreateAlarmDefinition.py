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
# Name    : CreateAlarmDefinition.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
import clr
clr.AddReference('System.Data')
from System.Data.SqlClient import SqlConnection
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System import Array
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

connString = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))

class AlarmState:
    Active = 'Active'
    Inactive = 'Inactive'


class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'SelectedMeasureName'
    Condition = 'Condition'
    ThresholdValue = 'ThresholdValue'
    Severity = 'Severity'  # AlarmLevel
    AlarmState = 'AlarmState'
    NECollection = 'NECollection'  # NeList
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'
    LookBack = 'LookbackMinutes'
    Schedule = 'Schedule'
    Aggregation = 'Aggregation'
    MeasureType = 'KPIOrCounter'
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


def scheduleIntervalError(ScheduleInterval, AggregationPeriod):
    AggPer = {
    "None": 0,
    "1 Hour": 60,
    "1 Day": 1440 
    }
    IntervalError = True
    if AggregationPeriod in AggPer:
       if int(ScheduleInterval)< AggPer[AggregationPeriod]:
           IntervalError = False
    
    return IntervalError




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
def getAlarmDefinitionsNames(dataTableName):
    alarmDefinitions = []
    if Document.Data.Tables.Contains(dataTableName):
        dataTable = Document.Data.Tables[dataTableName]
        rows = IndexSet(dataTable.RowCount, True)
        print('Number of alarm definitions = %d' % rows.Count)
        cursor = DataValueCursor.CreateFormatted(dataTable.Columns[AlarmColumn.AlarmName])
        for row in dataTable.GetRows(rows, cursor):
            alarmDefinitions.append(cursor.CurrentValue)
    return alarmDefinitions




# function
def writeAlarmDefinitionToDB(alarmDefinition):
    sql = ''
    sqlTemplate = '''
        INSERT INTO [dbo].[tblAlarmDefinitions]
                ([AlarmName]
                ,[AlarmType]
                ,[MeasureName]
                ,[Condition]
                ,[ThresholdValue]
                ,[Severity]
                ,[AlarmState]
                ,[SpecificProblem]
                ,[ProbableCause]
                ,[Schedule]
                ,[Aggregation]
                ,[MeasureType]
                ,[CollectionID])
            VALUES
                ('%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,'%s'
                ,%s)
        '''
    print('Alarm Definition = ', alarmDefinition)


    #create sql to add alarm to alarmdef
    #if its a collection then add to join table with id of collection
    #if its a single node, create the collection first in tblCollection, get selection id, add to alarm def
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
        alarmDefinition[-1] = "(Select CollectionID from tblCollection where CollectionName = '"+alarmDefinition[-1]+"')"
    sql += sqlTemplate % tuple(alarmDefinition)
    print('sql = %s' % sql)
    print(sql)
    writeToDB(sql)


def writeToDB(sql):
    connString = Document.Properties["ConnStringNetAnDB"]
    connection = SqlConnection(connString)
    connection.Open()
    command = connection.CreateCommand()
    command.CommandText = sql
    try:
        command.ExecuteReader()
        connection.Close()
    except Exception as e:
        print (e.message)
        Document.Properties['ValidationError'] = "Error when saving collection"


def resetValues():
    Document.Properties['AlarmName'] = ''
    Document.Properties['Severity'] = 'MINOR'
    Document.Properties['Schedule'] = '15'
    Document.Properties['ProbableCause'] = ''
    Document.Properties['SpecificProblem'] = ''
    Document.Properties["ValidationError"] = ''
    srcTable = Document.Data.Tables["Measure Mapping"]
    filt=Document.FilteringSchemes[0][srcTable][srcTable.Columns["Measure"]].As[ListBoxFilter]()
    filt.Reset()


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
    ScheduleInterval = Document.Properties['Schedule']
    AggregationPeriod = Document.Properties['Aggregation']
    

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
                currSystemArea = measureTableCur['System Area'].CurrentValue
        
            if (currNodeType != nodeType) or (currSystemArea!= systemArea):
                errorMessage  += "Please ensure system area and node type are correct."
                isValid = False
    elif Document.Properties[AlarmColumn.SingleOrCollection] == 'Collection':
        dp["NE Collection"] = Document.Properties[AlarmColumn.NECollection]

    #Check Aggregation Login
    if scheduleIntervalError(ScheduleInterval, AggregationPeriod) != True:
        errorMessage = "Aggregation cannot be less than schedule Interval"
        isValid = False
    

    EmptyFields = validateEmptyFeilds(dp)
    ENMValid = inValidForENM(dp["Alarm Name"], dp["Probable Cause Input"], dp["Specific Problem Input"])

    if len(EmptyFields)>0 :
        isValid = False
        errorMessage = " please provide Value for: " + str(EmptyFields)
    if ENMValid == False:
        isValid = False
        errorMessage  += ", please remove '#' or '?' from the input fields"

    Document.Properties['ValidationError'] = errorMessage
    return isValid

alarmDefinitionsDataTableName = 'Alarm Definitions'
alarmDefinitions = getAlarmDefinitionsNames(dataTableName=alarmDefinitionsDataTableName)

AlarmState = AlarmState()  # Create an enum to represent alarm states
AlarmColumn = AlarmColumn()  # Create an enum to represent column names
alarmColumns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName, AlarmColumn.Condition, AlarmColumn.ThresholdValue,
                AlarmColumn.Severity,  AlarmColumn.AlarmState, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause,
                AlarmColumn.Schedule, AlarmColumn.Aggregation, AlarmColumn.MeasureType,AlarmColumn.NECollection]

alarmDefinitionErrorPropertyName = 'AlarmDefinitionError'
Document.Properties[alarmDefinitionErrorPropertyName] = ''

newAlarmName = Document.Properties[AlarmColumn.AlarmName]
print Document.Properties[AlarmColumn.AlarmName]

if ValidateErrors():
    if newAlarmName in alarmDefinitions:
        Document.Properties["ValidationError"] = 'Error: Alarm Definition "%s" already exists' % newAlarmName
    else:
        Document.Properties[alarmDefinitionErrorPropertyName] = ''  # clear any existing warning
        Document.Properties['AlarmState'] = AlarmState.Inactive  # all new Alarms are Inactive by default


        writeAlarmDefinitionToDB(alarmDefinition=[Document.Properties[propertyName.replace('_', '')] for propertyName in alarmColumns])
        Document.Properties["ValidationError"] = ""
        resetValues()
        changeView()

Document.Data.Tables[alarmDefinitionsDataTableName].Refresh()
