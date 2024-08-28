# (c) Ericsson Inc. 2021 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : AlarmDefinitonsCreateModify.py
# Date    : 27/01/2021
# Revision: 3.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from System.Collections.Generic import List
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
import clr
clr.AddReference('System.Data')
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System import Array
from collections import OrderedDict
from Spotfire.Dxp.Application.Filters import ListBoxFilter
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Application.Filters import *
from System.Data.Odbc import OdbcConnection, OdbcCommand
from datetime import date
from System import Environment, Threading
from System import DateTime
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast


EniqDataSource = 'ENIQDataSourcesDropDown'

if Document.Properties['isEdit'] == "Edit":
    EniqDataSource = 'EniqName'



conn_string_eniq = "DSN=" + Document.Properties[EniqDataSource]+";Pooling=true;Max Pool Size=20;Enlist=true;FetchArraySize=100000;"
user_name = Threading.Thread.CurrentPrincipal.Identity.Name
notify = Application.GetService[NotificationService]()


class AlarmState:
    Active = 'Active'
    Inactive = 'Inactive'


class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'SelectedMeasureList'
    Severity = 'Severity'  
    AlarmState = 'AlarmState'
    TableTypePlaceHolder = 'TableType'
    NECollection = 'NECollection'  
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'
    Schedule = 'Schedule'
    Aggregation = 'Aggregation'
    MeasureType = 'MeasureType'
    SingleOrCollection = 'SingleOrCollection'
    SingleNodeValue = 'SingleNodeValue'
    NodeType = 'NodeType'
    LookBackVal = 'LookBackVal'
    LookBackUnit = 'LookBackUnit'
    DataRangeVal = 'DataRangeVal'
    DataRangeUnit = 'DataRangeUnit'
    PeriodDuration = 'PeriodDuration'
    TableName = 'TableName'
    EniqName = EniqDataSource

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
    conn_string = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)

def reset_all_filtering_schemes():
    """
    Resets filters for all data tables
    """
    for filtering_scheme in Document.FilteringSchemes:
        for data_table in Document.Data.Tables:
            filtering_scheme[data_table].ResetAllFilters()


def create_table(data_table_name, stream):
    """Create spotfire data table using a given table name and memory stream

    Arguments:
        data_table_name - string
        stream - memory stream
    """
    settings = TextDataReaderSettings()
    settings.Separator = ";"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)
    settings.SetDataType(0, DataType.String)
    settings.SetDataType(1, DataType.String)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(data_table_name):
        Document.Data.Tables[data_table_name].ReplaceData(fs)

    else:
        Document.Data.Tables.Add(data_table_name, fs)


def reset_values():
    """Resets UI inputs and filters to default values"""
    Document.Properties['AlarmName'] = ''
    Document.Properties['AlarmType'] = 'Threshold'
    Document.Properties['NECollection'] = ''
    Document.Properties['KPIType'] = 'Counter'
    Document.Properties['Severity'] = 'MINOR'
    Document.Properties['Schedule'] = '15'
    Document.Properties['Aggregation'] = 'None'
    Document.Properties['ProbableCause'] = ''
    Document.Properties['SpecificProblem'] = ''
    Document.Properties['SelectedMeasureList'] = ''
    Document.Properties['SelectedKPI1'] = ''
    Document.Properties['SelectedKPI2'] = ''
    Document.Properties['SelectedKPI3'] = ''
    Document.Properties['SelectedKPI4'] = ''
    Document.Properties['PeriodDuration'] = ''
    Document.Properties['TableType'] = ''
    Document.Properties['DataRangeUnit'] = 'ROP'
    Document.Properties['DataRangeVal'] = '1'
    Document.Properties['LookbackUnit'] = 'ROP'
    Document.Properties['LookbackVal'] = '1'
    src_table = Document.Data.Tables["Measure Mapping"]
    filt=Document.FilteringSchemes[1][src_table][src_table.Columns["Measure"]].As[ListBoxFilter]()
    filt.Reset()


def create_cursor(e_table):
    """Create cursors for a given table, these are used to loop through columns

    Arguments:
        e_table {data table} -- table object

    Returns:
        cursDict -- dictionary of cursors for the given table
    """
    curs_list = []
    col_list = []
    for eColumn in e_table.Columns:
        curs_list.append(DataValueCursor.CreateFormatted(e_table.Columns[eColumn.Name]))
        col_list.append(e_table.Columns[eColumn.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))
    return cusr_dict


def get_distinct_values_list(data_table, cursor, specified_column):
    """Get the values in a specified column from the data table

    Arguments:
        data_table -- table object
        cursor -- cursor object, used to loop through columns
        specified_column -- string name of desired column

    Returns:
        list_values -- list of values in the column

    """
    list_values = []
    rows = IndexSet(data_table.RowCount, True)
    for row in data_table.GetDistinctRows(rows,cursor[specified_column]):
        list_values.append(cursor[specified_column].CurrentValue)
    return list_values


def get_alarm_definitions_names(data_table_name):
    """Get Alarm Rule names stored in Alarm Definitions table

    Arguments:
        data_table_name {string} -- Alarm Definitions data table name

    Returns:
        alarm_definitions {list} -- list of Alarm Rules names

    """
    alarm_definitions = []
    if Document.Data.Tables.Contains(data_table_name):
        data_table = Document.Data.Tables[data_table_name]
        rows = IndexSet(data_table.RowCount, True)
        cursor = DataValueCursor.CreateFormatted(data_table.Columns[AlarmColumn.AlarmName])
        for row in data_table.GetRows(rows, cursor):
            alarm_definitions.append(cursor.CurrentValue)
    return alarm_definitions


def getEniqIdQuery(eniq_name):
    #function takes eniq_name as an argument and returns a select query to get the EniqId from database table tblEniqDs 
    sql = "(Select \"EniqID\" from \"tblEniqDS\" where \"EniqName\" = '"+eniq_name+"')"
    return sql

def generate_insert_SQL_template(alarm_definition,conn_string):
    """Set-up sql template to add Alarm Rule to NetAn data base
    
    Arguments:
        alarm_definition {enum} -- Alarm Rule details 
        conn_string {string} -- connection string to connect to NetAn database
    
    """


    
    sql = ''
    sql_template = '''
        INSERT INTO "tblAlarmDefinitions"
                ("AlarmName"
                ,"AlarmType"
                ,"MeasureName"
                ,"Severity"
                ,"SpecificProblem"
                ,"ProbableCause"
                ,"Schedule"
                ,"Aggregation"
                ,"MeasureType"
                ,"AlarmState"
                ,"TableName"
                ,"LookBackVal"
                ,"LookBackUnit"
                ,"DataRangeVal"
                ,"DataRangeUnit"
                ,"PeriodDuration"
                ,"EniqID"
                ,"TableType"
                ,"CollectionID"
                )
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
                ,%s
                ,'%s'
                ,%s
                ,'%s'
                ,'%s'
                ,%s
                ,'%s'
                ,%s);
        '''

    
    if Document.Properties[AlarmColumn.SingleOrCollection] == 'Single Node':

        selected_node_data_table_name = "NodeList"

        selected_node_data_table = Document.Data.Tables[selected_node_data_table_name]
        selected_node_cur = create_cursor(selected_node_data_table)

        node_type = get_distinct_values_list(selected_node_data_table, selected_node_cur, 'NodeType')
        system_area = Document.Properties['SystemArea']

        collection_name = Document.Properties['SingleNodeValue']
        collection_type = 'Single Node'

        created_by =  user_name
        created_on = str(date.today())

        sql_insert_collection_template = "INSERT INTO \"tblCollection\" (\"CollectionName\", \"NodeType\", \"SystemArea\", \"CollectionType\", \"CreatedBy\", \"CreatedOn\")" \
            "VALUES ('"+ collection_name  \
                + "','" \
                + node_type[0] \
                + "','" \
                + system_area \
                + "','" \
                + collection_type \
                + "','" \
                + created_by \
                + "','" \
                + created_on \
                +"');"
        sql += sql_insert_collection_template + " "

        if "PostgreSQL" in conn_string:
            alarm_definition[-1] = 'currval(\'\"tblCollection_seq\"\')'
        else:			
            alarm_definition[-1] = '(SELECT SCOPE_IDENTITY())'
        aggregation = Document.Properties['Aggregation']
        if aggregation == "1 Day": 
            alarm_definition[-2] = 'DAY'
        else:
            alarm_definition[-2] = 'RAW'
    elif Document.Properties[AlarmColumn.SingleOrCollection] == 'Subnetwork':

        '''selected_node_data_table_name = "NodeList"

        selected_node_data_table = Document.Data.Tables[selected_node_data_table_name]
        selected_node_cur = create_cursor(selected_node_data_table)

        node_type = get_distinct_values_list(selected_node_data_table, selected_node_cur, 'NodeType')'''
        node_type = Document.Properties['NodeType']
        system_area = Document.Properties['SystemArea']

        collection_name = Document.Properties['subnetwork']
        collection_type = 'Subnetwork'

        created_by =  user_name
        created_on = str(date.today())

        sql_insert_collection_template = "INSERT INTO \"tblCollection\" (\"CollectionName\", \"NodeType\", \"SystemArea\", \"CollectionType\", \"CreatedBy\", \"CreatedOn\", \"EniqID\")" \
            "VALUES ('"+ collection_name  \
                + "','" \
                + node_type \
                + "','" \
                + system_area \
                + "','" \
                + collection_type \
                + "','" \
                + created_by \
                + "','" \
                + created_on \
                + "'," \
                + getEniqIdQuery(alarm_definition[-3]) \
                +");"
        sql += sql_insert_collection_template + " "

        if "PostgreSQL" in conn_string:
            alarm_definition[-1] = 'currval(\'\"tblCollection_seq\"\')'
        else:			
            alarm_definition[-1] = '(SELECT SCOPE_IDENTITY())'
        aggregation = Document.Properties['Aggregation']
        if aggregation == "1 Day": 
            alarm_definition[-2] = 'DAY'
        else:
            alarm_definition[-2] = 'RAW'
    else:
        aggregation = Document.Properties['Aggregation']
        if aggregation == "1 Day": 
            alarm_definition[-2] = 'DAY'
        else:
            alarm_definition[-2] = 'RAW'
        alarm_definition[-1] = "(Select \"CollectionID\" from \"tblCollection\" where \"CollectionName\" = '"+alarm_definition[-1]+"')"
    
    alarm_definition[-3]=getEniqIdQuery(alarm_definition[-3])
    #print "alarm_definition: ", alarm_definition
    sql += sql_template % tuple(alarm_definition)
    print "sql"
    print sql
    write_to_db(sql)


def generate_update_SQL_template(alarm_definition,conn_string):
    """Set-up sql template to update Alarm Rule in NetAn data base

    Arguments:
        alarm_definition {enum} -- Alarm Rule details 
        conn_string {string} -- connection string to connect to NetAn database

    """
    sql = ''
    sql_template = '''
UPDATE "tblAlarmDefinitions"
   SET "AlarmType" = '%s'
      ,"MeasureName" = '%s'
      ,"Severity" = '%s'
      ,"SpecificProblem" = '%s'
      ,"ProbableCause" = '%s'
      ,"Schedule" = '%s'
      ,"Aggregation" = '%s'
      ,"MeasureType" = '%s'
      ,"AlarmState" = '%s'
      ,"TableName" = '%s'
      ,"LookBackVal" = %s
      ,"LookBackUnit" = '%s'
      ,"DataRangeVal" = %s
      ,"DataRangeUnit" = '%s'
      ,"PeriodDuration" = '%s'
      ,"EniqID"=%s
      ,"TableType" = '%s'
      ,"CollectionID" = %s
 WHERE "AlarmName" = '%s';
'''
    
    if Document.Properties[AlarmColumn.SingleOrCollection] == 'Single Node':

        selected_node_data_table_name = "NodeList"
        selected_node_data_table = Document.Data.Tables[selected_node_data_table_name]
        selected_node_cur = create_cursor(selected_node_data_table)

        node_type = get_distinct_values_list(selected_node_data_table, selected_node_cur, 'NodeType')
        system_area = Document.Properties['SystemArea']

        collection_name = Document.Properties['SingleNodeValue']
        collection_type = 'Single Node'

        sql_insert_collection_template = "INSERT INTO \"tblCollection\" (\"CollectionName\", \"NodeType\", \"SystemArea\", \"CollectionType\")" \
            "VALUES ('"+ collection_name  \
                + "','" \
                + node_type[0] \
                + "','" \
                + system_area \
                + "','" \
                + collection_type \
                +"');"
        sql += sql_insert_collection_template + " "

        if "PostgreSQL" in conn_string:
            alarm_definition[-1] = 'currval(\'\"tblCollection_seq\"\')'
        else:
            alarm_definition[-1] = '(SELECT SCOPE_IDENTITY())'
        aggregation = Document.Properties['Aggregation']
        if aggregation == "1 Day": 
            alarm_definition[-2] = 'DAY'
        else:
            alarm_definition[-2] = 'RAW'
    elif Document.Properties[AlarmColumn.SingleOrCollection] == 'Subnetwork':

        node_type = Document.Properties['NodeType']
        system_area = Document.Properties['SystemArea']

        collection_name = Document.Properties['subnetwork']
        collection_type = 'Subnetwork'
        
        created_by =  user_name
        created_on = str(date.today())

        sql_insert_collection_template = "INSERT INTO \"tblCollection\" (\"CollectionName\", \"NodeType\", \"SystemArea\", \"CollectionType\", \"CreatedBy\", \"CreatedOn\", \"EniqID\")" \
            "VALUES ('"+ collection_name  \
                + "','" \
                + node_type \
                + "','" \
                + system_area \
                + "','" \
                + collection_type \
                + "','" \
                + created_by \
                + "','" \
                + created_on \
                + "'," \
                + getEniqIdQuery(alarm_definition[-3]) \
                +");"
        sql += sql_insert_collection_template + " "

        if "PostgreSQL" in conn_string:
            alarm_definition[-1] = 'currval(\'\"tblCollection_seq\"\')'
        else:
            alarm_definition[-1] = '(SELECT SCOPE_IDENTITY())'
        aggregation = Document.Properties['Aggregation']
        if aggregation == "1 Day": 
            alarm_definition[-2] = 'DAY'
        else:
            alarm_definition[-2] = 'RAW'
    else:
        aggregation = Document.Properties['Aggregation']
        if aggregation == "1 Day": 
            alarm_definition[-2] = 'DAY'
        else:
            alarm_definition[-2] = 'RAW'
        alarm_definition[-1] = "(Select \"CollectionID\" from \"tblCollection\" where \"CollectionName\" = '"+alarm_definition[-1]+"')"
    alarm_definition[-3]=getEniqIdQuery(alarm_definition[-3])
    sql += sql_template % tuple(alarm_definition[1:] + alarm_definition[:1])
    write_to_db(sql)


def write_to_db(sql):
    """Add/update alarm rule in NetAn data base

    Arguments:
        sql {string} -- sql string template to add/update alarm rule

    """
    connection = OdbcConnection(conn_string)
    connection.Open()
    command = connection.CreateCommand()
    command.CommandText = sql
    try:
        command.ExecuteReader()
        connection.Close()
    except Exception as e:
        print "error", e.message
        Document.Properties['ValidationError'] = "Error when saving collection"


def get_table_name(measure_name, measure_mapping_data_table):
    """gets a measure's table name with a given measure name

    Arguments:
        measure_name -- string measure
        measure_mapping_data_table -- Measure Mapping data table object

    Returns:
        selected measure's table name
    """
    measure_filter = measure_mapping_data_table.Select("[Measure] = '" + measure_name + "'")
    if not measure_filter.AsIndexSet().IsEmpty:
        table_name_cursor = DataValueCursor.Create(measure_mapping_data_table.Columns['TABLENAME'])
        for row in measure_mapping_data_table.GetRows(measure_filter.AsIndexSet(),table_name_cursor):
            return table_name_cursor.CurrentValue


def get_period_duration(table_name):
    """
    Gets the Period Duration from ENIQ DB data table based on KPIs/counters selection

    Arguments:
        table_name {string} -- PM Data table name
    """
    query='SELECT max(PERIOD_DURATION) as "PERIOD_DURATION" FROM  ' + table_name
    db_settings = DatabaseDataSourceSettings("System.Data.Odbc", conn_string_eniq, query)
    ds = DatabaseDataSource(db_settings)
    new_data_table = Document.Data.Tables.Add('temp',ds)
    table=Document.Data.Tables['temp']
    cursor = DataValueCursor.CreateFormatted(table.Columns["PERIOD_DURATION"])
    val_data = List [str]();

    for row in table.GetRows(cursor):
        value = cursor.CurrentValue
        if value <> str.Empty:
            val_data.Add(value)

    val_data = List [str](set(val_data))
    val_data = ' '.join(val_data).split()
    val_data = ' '.join(val_data).split()
    Document.Properties['PeriodDuration'] = val_data[0]
    Document.Data.Tables.Remove(new_data_table)


def get_current_alarm_rule_id():
    """
    Gets current alarm rule id from NetAn DB based on alarm name

    """
    alarm_name = Document.Properties['AlarmName']
    
    query = """SELECT "AlarmID" as "AlarmId" FROM "vwAlarmDefinitions" where "AlarmName" = '{0}'""".format(alarm_name)
    db_settings = DatabaseDataSourceSettings("System.Data.Odbc", conn_string, query)
    ds = DatabaseDataSource(db_settings)
    new_data_table = Document.Data.Tables.Add('temp',ds)
    table=Document.Data.Tables['temp']
    cursor = DataValueCursor.CreateFormatted(table.Columns["AlarmId"])
    val_data = List [str]();
    
    for row in table.GetRows(cursor):
        value = cursor.CurrentValue
        if value <> str.Empty:
            val_data.Add(value)
    val_data = List [str](set(val_data))
    val_data = ' '.join(val_data).split()
    val_data = ' '.join(val_data).split()
    Document.Properties['AlarmId'] = val_data[0]
    Document.Data.Tables.Remove(new_data_table)


def change_table_extension(table_name):
    """Changes PM data table sufix from RAW to DAY

    Arguments:
        table_name {string} -- PM Data table name
    Returns:
        table_name {string} -- PM Data table name with replaced sufix
    """
    return table_name.replace('_RAW', '_DAY').replace('_DELTA', '_DAY')


def set_schedule_value(aggregation):
    """Set schedule value to match aggregation

    Arguments:
        aggregation {string} -- aggregation value from UI drop-down list
    """
    if aggregation == '1 Day':
        Document.Properties['Schedule'] = '1440'
    elif aggregation == '1 Hour':
        Document.Properties['Schedule'] = '60'
    else:
        Document.Properties['Schedule'] = '15'


def main():
    ''' main function to add new Alarm Rules or edit existing Alarm Rules in NetAn DB'''
    try:
        if Document.Properties["Action"] == "Create":

            if new_alarm_name in alarm_definitions:
                Document.Properties["ValidationError"] = 'Error: Alarm Definition "%s" already exists' % new_alarm_name

            else:
                Document.Properties["ValidationError"] = ""
                table_name = Document.Properties["TableName"]
                table_name = table_name.upper()
                table_name = table_name.split(',')[0]
                aggregation = Document.Properties['Aggregation']
                if aggregation == '1 Day':
                    table_name = change_table_extension(table_name)
            
                get_period_duration(table_name)
                Document.Properties[alarm_definition_error_property_name] = ''  
                Document.Properties['AlarmState'] = alarm_state.Inactive
              
                generate_insert_SQL_template(alarm_definition=[Document.Properties[propertyName.replace('_', '')] for propertyName in alarm_columns],conn_string=conn_string)
                
                get_current_alarm_rule_id()
                reset_all_filtering_schemes()
                reset_values()

        if Document.Properties["Action"] == "Edit":

            table_name = Document.Properties["TableName"]
            table_name = table_name.upper()
            table_name = table_name.split(',')[0]
            aggregation = Document.Properties['Aggregation']
            if aggregation == '1 Day':
                table_name = change_table_extension(table_name)

            get_period_duration(table_name)
            Document.Properties[alarm_definition_error_property_name] = ''  
            generate_update_SQL_template(alarm_definition=[Document.Properties['SelectedMeasureName'] if propertyName == 'MeasureName' else Document.Properties[propertyName.replace('_', '')] for propertyName in alarm_columns],conn_string = conn_string)
            reset_all_filtering_schemes()
            reset_values()

    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)

alarm_definitions_data_table_name = 'Alarm Definitions'

alarm_definitions = get_alarm_definitions_names(data_table_name=alarm_definitions_data_table_name)

alarm_state = AlarmState()  
alarm_column = AlarmColumn()  
alarm_columns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName,
                AlarmColumn.Severity, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause,
                AlarmColumn.Schedule, AlarmColumn.Aggregation, AlarmColumn.MeasureType, AlarmColumn.AlarmState, AlarmColumn.TableName,
                AlarmColumn.LookBackVal, AlarmColumn.LookBackUnit, AlarmColumn.DataRangeVal, AlarmColumn.DataRangeUnit, 
                AlarmColumn.PeriodDuration, AlarmColumn.EniqName, AlarmColumn.TableTypePlaceHolder, AlarmColumn.NECollection]




alarm_definition_error_property_name = 'AlarmDefinitionError'

Document.Properties[alarm_definition_error_property_name] = ''

new_alarm_name = Document.Properties[alarm_column.AlarmName]

aggregation = Document.Properties['Aggregation']

set_schedule_value(aggregation)

main()

Document.Data.Tables[alarm_definitions_data_table_name].Refresh()
Document.Properties["SaveColsScript"] = DateTime.UtcNow 