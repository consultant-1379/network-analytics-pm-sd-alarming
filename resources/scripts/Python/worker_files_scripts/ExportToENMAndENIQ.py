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
# Name    : ExportTOENMAndENIQ.py
# Date    : 09/12/2021
# Revision: 1.0
# Purpose : Exports triggered alarm rules to ENM through REST
#
# Usage   : PM Alarm
#

import clr
clr.AddReference('System.Data')
clr.AddReference('System.Web.Extensions')
import System.Web
import ast
import logging
from System.IO import StreamReader, File, Directory
from System.Data import DataSet
from System import Uri, Array, DateTime
from System.Net import ServicePointManager, SecurityProtocolType, WebRequest, CookieContainer
from System.Data.Odbc import OdbcConnection, OdbcType, OdbcDataAdapter
from datetime import datetime
from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from System.IO import StreamWriter, MemoryStream, SeekOrigin
import re
import json
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data.Formatters import *
ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12




ALL_COLUMN_TYPES = {
    "DateTime": DataType.DateTime,
    "Boolean": DataType.Boolean,
    "Byte": DataType.Integer,
    "Char": DataType.String,
    "Decimal": DataType.SingleReal,
    "Double": DataType.Real,
    "Guid": DataType.String,
    "Int16": DataType.Integer,
    "Int32": DataType.Integer,
    "Int64": DataType.Integer,
    "SByte": DataType.Undefined,
    "Single": DataType.SingleReal,
    "TimeSpan": DataType.TimeSpan,
    "UInt16": DataType.Integer,
    "UInt32": DataType.LongInteger,
    "UInt64": DataType.LongInteger
}

ALARM_TABLE_COLUMNS = [
    'AlarmName',
    'ReportTitle',
    'OssName',
    'ObjectOfReference',
    'ManagedObjectInstance',
    'PerceivedSeverityText',
    'AdditionalText',
    'MonitoredAttributeValues',
    'ThresholdInformation',
    'DATETIME_ID',
    'TIMELEVEL'
]

ERROR_TABLE_COLUMNS = [
    'ENMHostname', 
    'ErrorDetail', 
    'AlarmName',
    'ManagedObjectInstance',
    'ObjectOfReference',
    'OssName',
    'ReportTitle',
    'EventTime'
]

custom_date_format = 'yyyy-MM-dd HH:mm:ss'
DATETIME_FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
DATETIME_FORMATTER.FormatString = custom_date_format

custom_date_format = 'yyyy-MM-dd'
DATE_FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
DATE_FORMATTER.FormatString = custom_date_format

def create_cursor(table):
    """Create cursors for a given table, these are used to loop through columns"""
    
    curs_list = []
    col_list = []

    for column in table.Columns:
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name], DATETIME_FORMATTER))
            col_list.append(table.Columns[column.Name].ToString())
        else:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name]))
            col_list.append(table.Columns[column.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict


per_Eniq_Ds_Eniq_alarm_dict ={}

ALARM_SCHEDULE = Document.Properties['AlarmSchedule']

tables_to_keep = ["Alarm Definitions","empty_data_table", "vwEniqEnm", "Data Table","Successful_Alarms", "Failed_Alarms"] 

logger = logging.getLogger('root')
current_script = 'ExportToENMAndENIQ'
logging.basicConfig(
    format="""%(asctime)s|%(levelname)s|{current_schedule}|{current_script}|%(message)s""".format(current_schedule=ALARM_SCHEDULE, current_script=current_script),
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

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


def createRequest(url, method):
    """Function that accepts url and method (POST,PUT) and creates request, and stores session

    Arguments:
        url {Uri} -- url used to connect to ENM
        method {string} -- defines whether a POST or PUT request is needed

    Returns:
        request -- [description]
    """
    request = WebRequest.Create(url)
    request.Method = method
    request.Timeout = 50000
    request.Accept = "application/json"
    request.ContentType = "application/json"
    request.CookieContainer = CookieContainer()
    return request


def createCookies(request, urlLogin):
    """Gets cookies for current session

    Arguments:
        request {httprequest} -- current connection to enm - created by createRequest()
        urlLogin {Uri} -- url used to connect to ENM

    Returns:
        cookies -- returns current cookies for session
    """
    cookies = request.CookieContainer.GetCookies(urlLogin)
    return cookies


def putRequest(request, cookies, json):
    """For the session, send json data to the server - in this case send to ENM

    Arguments:
        request {httprequest} -- current connection to enm - created by createRequest()
        cookies {cookies} -- current cookies for session
        json {string} -- JSON used to send details to ENM
    """
    request.CookieContainer.Add(cookies)
    buffer = System.Text.Encoding.ASCII.GetBytes(json)
    streamWriter = request.GetRequestStream()
    streamWriter.Write(buffer, 0, len(buffer))
    streamWriter.Close()


def getResponse(response):
    """For any request, get the response stream and then close out streams

    Arguments:
        response {httpresponse} -- result of the response of the request
    """
    stream = response.GetResponseStream()
    streamReader = StreamReader(stream)
    content = streamReader.ReadToEnd()

    streamReader.Close()
    response.Close()
    stream.Close()


def clear_tables(table_names):
    for table in Document.Data.Tables:
        if table.Name not in table_names:
            Document.Data.Tables.Remove(Document.Data.Tables[table.Name])


def create_value_list_for_sql(alarm_dict, column_list):
    """ create a string in the format of (?,?,?)etc. so that the correct amount of command parameters can be added."""
    overall_rows = []
    for sql_column in alarm_dict.items():
        value_list = []
        current_row = ""

        for i in range(len(column_list)):
            value_list.append('?')

        current_row = """({0})""".format(','.join(value_list))
        overall_rows.append(current_row)

    return ','.join(overall_rows)


def create_data_table(table_name, text_data, column_names, column_types):
    """ creates a data table using a text source """
   
    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine('|'.join(column_names) + '\r\n')
    writer.Flush()

    for line in text_data:
        writer.WriteLine(line)

    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = "|"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)

    for i in range(len(column_types)):
        settings.SetDataType(i, column_types[i])

    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(table_name):
        Document.Data.Tables.Remove(Document.Data.Tables[table_name])
    Document.Data.Tables.Add(table_name, fs)



def apply_parameters(command, parameters, column_list):
    """ for an ODBC command, add all the required values for the parameters. varchar is default value."""

    parameter_index = 0
    for col,col_value in parameters.items():
        # need to be added in correct order, so use the column_list to define the order
        for alarm_col in column_list:         
            command.Parameters.Add("@col"+str(parameter_index), OdbcType.VarChar).Value = str(col_value[alarm_col])
            parameter_index += 1

    return command


def sqlEniq(sql, parameters, column_list, eniq_name = None):
    """ Run a SQL query against the ENIQ server using ODBC connection """
    try:
        conn_string = "DSN=" + eniq_name
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, parameters, column_list)
        command.ExecuteNonQuery()

        connection.Close()
    except Exception as e:
        logger.error(e.message)
        raise


def getTheTableName(eniq):
    try:
        sql = "SELECT TOP 1 * FROM DC_Z_ALARM_NETAN_RAW"
        dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + eniq, sql)
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        dt = Document.Data.Tables.Add('temp', dataTableDataSource)
        Document.Data.Tables.Remove('temp')
        return 'DC_Z_ALARM_NETAN_RAW'
    except Exception as e:
        logger.info('DC_Z_ALARM_NETAN_RAW is not present in the DB')
        return 'DC_Z_ALARM_INFO_RAW'

def log_error_message(enm_error_list, eniq_name):
    """Log error messages for connection issues, json issues etc. """
    dc_z_table_name = getTheTableName(eniq_name)
    dc_z_table = re.search('DC_Z_ALARM_(.*?)_RAW' ,dc_z_table_name).group(1)
    sql_insert_template = '''
    INSERT INTO DC_Z_ALARM_{comp}_ERROR ({error_columns}) VALUES 
    '''.format(error_columns=','.join(ERROR_TABLE_COLUMNS),comp= dc_z_table)

    sql_insert_template += create_value_list_for_sql(enm_error_list, ERROR_TABLE_COLUMNS)

    sqlEniq(sql_insert_template, enm_error_list, ERROR_TABLE_COLUMNS, eniq_name)


def log_alarm(eniq_alarm_list, eniq_name):
    """Log all alarms that have breached the threshold"""

    curr_partition = findCurrentPartition(eniq_name)

    sql_insert_template = '''
            INSERT INTO  {curr_partition}  ({alarm_columns}) VALUES 
            '''.format(curr_partition=curr_partition, alarm_columns=','.join(ALARM_TABLE_COLUMNS))
       
    sql_insert_template += create_value_list_for_sql(eniq_alarm_list, ALARM_TABLE_COLUMNS)
    
    sqlEniq(sql_insert_template, eniq_alarm_list, ALARM_TABLE_COLUMNS, eniq_name)


def findCurrentPartition(dataSourceName):
    """ Find current partition for DC_ALARM_INFO so that the insert will use the correct table"""
    dc_z_table_name = getTheTableName(eniq_name)
    dc_z_table = re.search('DC_Z_ALARM_(.*?)_RAW' ,dc_z_table_name).group(1)
    sql = "SELECT tableName FROM DWHPARTITION WHERE CURRENT DATE BETWEEN STARTTIME AND ENDTIME AND STORAGEID IN ('DC_Z_ALARM_{comp}:RAW')".format(comp = dc_z_table)
    conn_string = "DSN=" + dataSourceName + "repdb"
    connection = OdbcConnection(conn_string)
    connection.Open()
    command = connection.CreateCommand()
    command.CommandText = sql
    reader = command.ExecuteReader()
    loopguard = 0
    while reader.Read() and loopguard != 1:
        currentPartition = reader[0]
        loopguard = 1
    connection.Close()
    return currentPartition


def get_column_names_and_types(data_set):
    """ for a dataset object, return the column types(refering to a type dict) and names"""

    column_types = []
    column_names = []

    for column in data_set.Tables[0].Columns:
        column_names.append(column.ColumnName)
        col_type = column.DataType.Name.ToString()

        if column.ColumnName == 'DATE_ID':
            column_types.append(DataType.Date)
        elif (ALL_COLUMN_TYPES.get(col_type)):
            column_types.append(ALL_COLUMN_TYPES[col_type])
        else:
            column_types.append(DataType.String)

    return column_names, column_types  


def generate_text_data(data_set, column_names):
    """ generator function - yields rows of data for a dataset object loaded with sql tables """
    
    curr_row = ""
    for row in data_set.Tables[0].Rows:
        curr_row = []
        for col in column_names:
            curr_row.append(str(row[col]))
        yield "%s\r\n" % ('|'.join(curr_row))

def runQuery(sql, connString):
    try:
        table_name = "tblENM"
        connection = OdbcConnection(connString)
        connection.Open()
        data_set = DataSet()
        adaptor = OdbcDataAdapter(sql, connection)
        adaptor.Fill(data_set)
        
        logger.info("querry Successful to table in netAn DB: " + table_name)
        logger.info("Number of rows returned: "+str(data_set.Tables[0].Rows.Count))
        connection.Close()      
        return data_set, table_name

    except Exception as e:
        logger.error("Failed to pull data for: " + table_name)
        logger.error(e.message)
        return None



def getEnmTable():
    #the function forms sql query to get the eniq password from netan DB
    connString = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))    
    sql = "select \"EnmUrl\", \"EnmUsername\", \"EnmPassword\", \"OssId\", \"EniqID\" From \"tblENM\""

    ds, tableName = runQuery(sql, connString)
    try: 
        column_names, column_types = get_column_names_and_types(ds)
        text_data = generate_text_data(ds, column_names)
        create_data_table(tableName, text_data, column_names, column_types)
        logger.info("table created for tblENM :" + tableName)
    except Exception as e:
        logger.error("Failed to create data table : " + tableName)
        logger.error(e.message)



def connectionREST(connectionDetails):
    try:
        urlLogin = Uri("https://" + connectionDetails['serverName'] + "/login?IDToken1=" + connectionDetails['userName'] +"&IDToken2=" + connectionDetails['password'])
        intialRequest = createRequest(urlLogin, "POST")
        intialResponse = intialRequest.GetResponse()

        cookies = createCookies(intialRequest, urlLogin)
        getResponse(intialResponse)
    except Exception as e:
        errorDetail = e.message
        logger.error("Error making connection to ENM host: " + connectionDetails['serverName'])
        logger.error(errorDetail)

    return cookies


def get_column_val(cursor):
    try:
       return  cursor.CurrentValue
    except:
        return None
        

def createCursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""
    
    customDateFormat='yyyy-MM-dd HH:mm:ss'
    formatter=DataType.DateTime.CreateLocalizedFormatter()
    formatter.FormatString = customDateFormat
    

    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        if eColumn.Name in ['DATETIME_ID', 'DATE_ID']:
            cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name],formatter))
            colList.append(eTable.Columns[eColumn.Name].ToString())
        else: 
            cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
            colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))
    return cusrDict



def getEniqId(eniqName):
    eniqId = ""
    table=Document.Data.Tables["vwEniqEnm"]
    rowSelection=table.Select('EniqName = "'+eniqName+'"')
    cursor = DataValueCursor.CreateFormatted(table.Columns["EniqID"])
    for row in table.GetRows(rowSelection.AsIndexSet(),cursor):
        eniqId = cursor.CurrentValue
        break
    return eniqId


def getServerDetails(ossId, eniqName):
    #the function will take eniq_name and oss_id and will find rest of the info from vwEniqEnmm and tblENM table

    connectionDetails = {}
    eniqId = getEniqId(eniqName)
    table=Document.Data.Tables["tblENM"]

    #filter the table tblENM with eniqId and ossid
    rowSelectionEniq=table.Select('EniqID = '+str(eniqId)+'')
    rowSelectionOssId=table.Select('OssId = "'+ossId+'"')  
    rows = RowSelection.Combine(rowSelectionEniq, rowSelectionOssId, DataSelectionOperation.Intersect).AsIndexSet()
    tableENMCursor = createCursor(table)    
    for row in table.GetRows(rows, Array[DataValueCursor](tableENMCursor.values())):
        connectionDetails['serverName']= get_column_val(tableENMCursor["EnmUrl"])
        connectionDetails['password'] = decrypt(get_column_val(tableENMCursor["EnmPassword"]))
        connectionDetails['userName'] = get_column_val(tableENMCursor['EnmUsername'])
    return connectionDetails



def create_dict_per_eniq(dict_data):
    #function takes dictionary of dictionary items 
    #returns dictionary of given data grouped per eniq
    per_Eniq_Ds_info = {}
    for index, alarm_info in dict_data.items():
        tmp = {}
        if alarm_info["eniqName"] in per_Eniq_Ds_info:
            tmp[len(per_Eniq_Ds_info[alarm_info["eniqName"]].keys())+1] = alarm_info
            per_Eniq_Ds_info[alarm_info["eniqName"]].update(tmp)
     
        else:
            tmp[1] =alarm_info
            per_Eniq_Ds_info[alarm_info["eniqName"]] = tmp
            
    return per_Eniq_Ds_info


def valid_object_ref(text,element):
    core=['SubNetwork', 'MeContext', 'ManagedElement']
    if any([c in text for c in core]) and element in text:
        return True
    else:
        return False


def check_alarm_or_event(alarm):
    """ checks if given alarm is an alarm or event"""

    alarm_def_dt_name = 'Alarm Definitions'
    alarm_filtercondition = "[AlarmName] = '"+alarm+"'"
    alarm_def_dt, alarm_def_cursor, alarm_index = get_spotfire_dt(alarm_def_dt_name, alarm_filtercondition)
    is_alarm = ""
    for selectedmeasure in alarm_def_dt.GetRows(alarm_index, Array[DataValueCursor](alarm_def_cursor.values())):
        is_alarm = alarm_def_cursor["AlarmOrEvent"].CurrentValue
    return is_alarm


def get_spotfire_dt(source_data_table_name, filtercondition):
    """ for a given spotfire table return the tablename, cursor and indexset """

    source_data_table = Document.Data.Tables[source_data_table_name]
    source_cur = create_cursor(source_data_table)
    
    if filtercondition != 'None':
        index_filter = Document.Data.Tables[source_data_table_name].Select(filtercondition)
        index_set = index_filter.AsIndexSet()

        return source_data_table, source_cur, index_set

    return source_data_table, source_cur


    
# get alarm defintion table
try:
    getEnmTable()
    alarmDefinitionsDataTableName = 'Alarm Definitions'
    alarmDefinitionsDataTable = Document.Data.Tables[alarmDefinitionsDataTableName]
    alarmDefinitionsDataTableCur = createCursor(alarmDefinitionsDataTable)

    alarmTableName = 'Data Table'
    alarmTable = Document.Data.Tables[alarmTableName]
    alarmTableCursor = createCursor(alarmTable)

    # set rowcount for import
    rowCount = alarmTable.RowCount
    rowsToInclude = IndexSet(rowCount, True)

    # values used to create ENIQ queries after sent to ENM
    eniq_alarm_list = {}
    eniq_dict_index = 1
    enm_error_list = {}
    enm_dict_index = 1
    alarm_success_count = 0
    alarm_json_dict = {}
    alarm_json_dict_index = 1

    ossid=""
    dateTimeID = False
    dateID = False
    hourID = False
    hourValue = ""

    if "DATETIME_ID" in alarmTableCursor.keys():
        dateTimeID = True        
    if "DATE_ID" in alarmTableCursor.keys():
        dateID = True       
    if "HOUR_ID" in alarmTableCursor.keys():
        hourID = True

    column_list = [str(alarm_column) for alarm_column in alarmTable.Columns]

    # loop through each alarm in PM alarm table (i.e. alarms that broke threshold) and find corresponding details in alarm definitions table
    for row in alarmTable.GetRows(Array[DataValueCursor](alarmTableCursor.values())):
        alarmName = get_column_val(alarmTableCursor["ALARM_NAME"])
        thresholdInfo = get_column_val(alarmTableCursor["THRESHOLDINFORMATION"])
        monitoredAttributesValues = get_column_val(alarmTableCursor["MEASUREVALUE"])
        severity = get_column_val(alarmTableCursor["SEVERITY"])

        elementValue = ""
        if "ELEMENT" in column_list:            
            elementValue = get_column_val(alarmTableCursor["ELEMENT"])
        sn = ""
        if "SN" in column_list:
            sn=get_column_val(alarmTableCursor["SN"])
        
        moid = ""
        if "MOID" in column_list:
            moid=get_column_val(alarmTableCursor["MOID"])
        
        objectOfReference = get_column_val(alarmTableCursor["ObjectOfReference"])
        managedObjectInstance = moid  #same as objectOfReference
        eventTypeDesc = 'Quality Of Service'
        timeZone = get_column_val(alarmTableCursor["DC_TIMEZONE"])

        if dateTimeID:
            alarmTableEventTime = get_column_val(alarmTableCursor["DATETIME_ID"])
            date_field = "DATETIME_ID"
            if alarmTableEventTime == "(Empty)":
                alarmTableEventTime = get_column_val(alarmTableCursor["DATE_ID"])
                date_field = "DATE_ID"
        else:
            alarmTableEventTime = get_column_val(alarmTableCursor["DATE_ID"])
            date_field = "DATE_ID"
        if hourID:
            hourValue = get_column_val(alarmTableCursor["HOUR_ID"])
            if hourValue == '(Empty)':
                hourValue = ''
        ossid = get_column_val(alarmTableCursor["OSS_ID"])
        
        managedObjectClass = alarmName
        
        
        
        for alarmDefRow in alarmDefinitionsDataTable.GetRows(Array[DataValueCursor](alarmDefinitionsDataTableCur.values())):
            currentAlarmName = get_column_val(alarmDefinitionsDataTableCur["AlarmName"])
            if currentAlarmName == alarmName:
                
                specificProblem = get_column_val(alarmDefinitionsDataTableCur["SpecificProblem"])
                probableCause = get_column_val(alarmDefinitionsDataTableCur["ProbableCause"])
                eventType = get_column_val(alarmDefinitionsDataTableCur["AlarmType"])
                perceivedSeverity = get_column_val(alarmDefinitionsDataTableCur["Severity"])
                additionalText = alarmName
                monitoredAttributes = get_column_val(alarmDefinitionsDataTableCur["MeasureName"])
                eniqName = get_column_val(alarmDefinitionsDataTableCur["EniqName"])
                
                if check_alarm_or_event(alarmName) == "Event":
                    connectionDetails = getServerDetails(ossid, eniqName)
                    #ask do we need this cookies thing after multiple ENM connections
                    
                    cookies = connectionREST(connectionDetails)     
                                
                    urlSendAlarm = Uri("https://" + connectionDetails['serverName'] + "/errevents-service/v1/errorevent/" + objectOfReference + "," + specificProblem + ","
                                        + probableCause + "," + eventType)
                    json_event = "{\"ManagedObjectClass\":\"" + managedObjectClass + "\", \
                            \"ETText\":\"" + eventTypeDesc + "\", \
                            \"PCText\":\"" + probableCause + "\",\
                            \"SPText\":\"" + specificProblem + "\", \
                            \"perceivedSeverity\":\"" + perceivedSeverity + "\", \
                            \"PerceivedSeverityText\":\"" + perceivedSeverity + "\", \
                            \"ThresholdInformation\":\"" + thresholdInfo + "\", \
                            \"AdditionalText\":\"" + additionalText + "\", \
                            \"AdditionalInformation\":\"" + additionalText + "\", \
                            \"ManagedObjectInstance\":\"" + managedObjectInstance + "\", \
                            \"EventTime\":\"" + alarmTableEventTime + "\", \
                            \"MonitoredAttributes\":\"" + monitoredAttributes + "\", \
                            \"MonitoredAttributeValues\":\"" + monitoredAttributesValues + "\", \
                            \"OssName\":\"" + ossid + "\", \
                            \"TimeZone\":\"" + timeZone + "\"} \
                        "
                            
                    
                    try:
                        
                        requestTest = createRequest(urlSendAlarm, "PUT")
                        putRequest(requestTest, cookies, json_event)
                        response = requestTest.GetResponse()
                        getResponse(response)

                        logger.info("Alarm sent succesfully to ENM: " + alarmName)
                        alarm_success_count += 1
                    
                        eniq_alarm_list[eniq_dict_index] = {
                            'AlarmName': alarmName,
                            'ReportTitle': 'PM Alarm',
                            'OssName': ossid,
                            'ObjectOfReference': objectOfReference,
                            'ManagedObjectInstance': managedObjectInstance,
                            'PerceivedSeverityText': perceivedSeverity,
                            'AdditionalText': monitoredAttributes,
                            'MonitoredAttributeValues': monitoredAttributesValues,
                            'ThresholdInformation': thresholdInfo,
                            'DATETIME_ID': alarmTableEventTime,
                            'TIMELEVEL': hourValue,
                            'eniqName': eniqName

                        }
                        eniq_dict_index += 1
                        
                    except Exception as e:
                        logger.error("Failed to send alarm to ENM: " + alarmName)
                        logger.error(e.message)
                        
                        errorDetail = e.message.replace("'","") + ", URL:" + str(urlSendAlarm) + ", JSON:" + str(json_event)
                        
                        enm_error_list[enm_dict_index] = {
                            'ENMHostname':connectionDetails['serverName'], 
                            'ErrorDetail': errorDetail, 
                            'AlarmName': alarmName,
                            'ManagedObjectInstance':managedObjectInstance,
                            'ObjectOfReference': objectOfReference,
                            'OssName':ossid,
                            'ReportTitle':'PM Alarm',
                            'EventTime':alarmTableEventTime,
                            'eniqName': eniqName
                        }
                        enm_dict_index += 1
                else:
                    perceivedSeverity = severity
                    json_data= {
                        "specificProblem": specificProblem,
                        "probableCause": probableCause,
                        "eventType": "",
                        "perceivedSeverity": perceivedSeverity,
                        "recordType": "ALARM",
                        "managedObjectInstance": objectOfReference,#ManagementSystem=ENM
                        "additionalAttributes": {"additionalText":additionalText,"fdn":objectOfReference,"behalf":"NetAn PMA"}
                    }
                    
                    alarm_json_dict[alarmName+"_row_"+str(alarm_json_dict_index)] = json_data
                    alarm_json_dict_index += 1


    
except Exception as e:
    logger.error("Error while parsing through alarm list.")
    logger.error(e.message)            
    clear_tables(tables_to_keep)


# create directory for msmt exports
if not Directory.Exists("C:\\NetAn_PMA_Exports"):
    Directory.CreateDirectory("C:\\NetAn_PMA_Exports")

fileName = "PMA_"+str(datetime.today().strftime('%Y%m%d%H%M%S'))+".json"

if alarm_json_dict:
    with open("C:\\NetAn_PMA_Exports\\"+fileName, "w") as outfile:
        try:
            json.dump(alarm_json_dict, outfile)
            Document.Properties["JSONFileName"] = fileName
            Document.Properties["ExecutePythonDataFunction"] = DateTime.UtcNow
            logger.info("Alarm JSON file written to C:\\NetAn_PMA_Exports\\"+fileName)
        except Exception as e:
            logger.error("Failed to write to JSON file")
            logger.error(e.message)


if eniq_alarm_list:
    #create each eniq as key for a dictionary items
    per_Eniq_Ds_Eniq_alarm_dict = create_dict_per_eniq(eniq_alarm_list)
    for eniq_name, alarms in per_Eniq_Ds_Eniq_alarm_dict.items():
        try:
            log_alarm(alarms, eniq_name)
            logger.info("Alarms tracked in ENIQ")
        except Exception as e:
            logger.error("Failed to send alarm info for ENIQ : " )
            logger.error(e.message)


if enm_error_list:
    per_Eniq_Ds_Enm_dict = create_dict_per_eniq(enm_error_list)

    for eniq_name, enm_errors in per_Eniq_Ds_Enm_dict.items():
        try:
            log_error_message(enm_errors, eniq_name)
            logger.info("ENM errors tracked in ENIQ")
        except Exception as e:
            logger.error("Failed to send alarm errors to ENIQ.")
            logger.error(e.message)

