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
# Name    : FetchDataTables.py
# Date    : 12/05/2021
# Revision: 2.0
# Purpose :
#
# Usage   : PM Alarming
#

import time
import clr
clr.AddReference('System.Data')
from System.Data.Odbc import OdbcConnection, OdbcDataAdapter
from System.Data import DataSet
from System import Array, Object
from multiprocessing.pool import ThreadPool
from datetime import datetime
from Spotfire.Dxp.Data.Formatters import *
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System.Collections.Generic import Dictionary
from System import DateTime
import logging
import sys

from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data import DataType
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Application.Scripting import ScriptDefinition


# global vars

POOL = ThreadPool(30)

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

PLACEHOLDER_EXPRESSIONS = {
    "LongInteger": "0",
    "DateTime": "DATETIME('01/01/2021')",
    "Integer": "0",
    "String": "'Placeholder'",
    "Real": "0.0",
    "SingleReal": "0.0",
    "Boolean": "True",
    "LongInteger": "0",
    "Time": "Time(1)",
    "TimeSpan": "TimeSpan(1)",
    "(Empty)":"'0'"
}

custom_date_format = 'yyyy-MM-dd HH:mm:ss'
DATETIME_FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
DATETIME_FORMATTER.FormatString = custom_date_format

custom_date_format = 'yyyy-MM-dd'
DATE_FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
DATE_FORMATTER.FormatString = custom_date_format

ALARM_SCHEDULE = Document.Properties['AlarmSchedule']
logger = logging.getLogger('root')

current_script = 'FetchDataTables'
logging.basicConfig(
    format="""%(asctime)s|%(levelname)s|{current_schedule}|{current_script}|%(message)s""".format(current_schedule=ALARM_SCHEDULE,current_script=current_script),
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) )
        else:
            print '%r  %2.2f s' % \
                  (method.__name__, (te - ts) )
        return result
    return timed


def clear_tables(table_names):
    for table in Document.Data.Tables:
        if table.Name not in table_names:
            Document.Data.Tables.Remove(Document.Data.Tables[table.Name])


def create_cursor(table):
    """Create cursors for a given table, these are used to loop through columns"""
    
    curs_list = []
    col_list = []

    for column in table.Columns:
        if column.Properties.DataType.ToString() in ['DateTime']:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name], DATETIME_FORMATTER))
            col_list.append(table.Columns[column.Name].ToString())
        else:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name]))
            col_list.append(table.Columns[column.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict


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


@timeit
def generate_text_data(data_set, column_names):
    """ generator function - yields rows of data for a dataset object loaded with sql tables """
    
    curr_row = ""
    
    for row in data_set.Tables[0].Rows:
        curr_row = []
        for col in column_names:
            curr_row.append(str(row[col]))
        yield "%s\r\n" % ('|'.join(curr_row))


def check_alarm_can_run(alarm_name):
    """ based on the ready for alarming table, these are the alarms from the list of active alarms that can be run """

    table_name = 'Ready For Alarming'
    filtercondition="[AlarmName] = '"+ alarm_name +"'"
    ready_alarm_dt, ready_alarm_cursor, ready_alarm_index = get_spotfire_dt(table_name, filtercondition)

    return [ready_alarm_cursor['AlarmName'].CurrentValue for selectedmeasure in ready_alarm_dt.GetRows(ready_alarm_index, Array[DataValueCursor](ready_alarm_cursor.values()))]


def replace_date_time(sql, date_value):
    """ this function will set the datetime value in the query to whatever was in the log load status table"""

    query = '''declare @dateTime datetime 
              set @dateTime='{0}'
               {1}
               '''.format(date_value, sql)
    return query

def FetchNodesForSubnetwork(subnetworkName):
    AlarmCollection = Document.Data.Tables['Alarm Collections']
    subnetworkTable = Document.Data.Tables['SubNetwork List']
    collection_cur = create_cursor(AlarmCollection)
    filter_collection = AlarmCollection.Select("[CollectionName] = '" + subnetworkName + "'")
    node_list = ''  
    ne_list =[]    
    for row in AlarmCollection.GetRows(filter_collection.AsIndexSet(),Array[DataValueCursor](collection_cur.values())):
        nodeTypeSub = collection_cur['NodeType'].CurrentValue
        systemAreaSub = collection_cur['SystemArea'].CurrentValue
        EniqServer = collection_cur['EniqName'].CurrentValue
        subnetwork_cur = create_cursor(subnetworkTable)
        subnetwork_filter = subnetworkTable.Select("([NodeType] = '" + nodeTypeSub + "' and [SystemArea] = '" + systemAreaSub + "' and [DataSourceName] = '" + EniqServer + "' and [SubnetworkName] = '"+ subnetworkName +"')")
        for node in subnetworkTable.GetRows(subnetwork_filter.AsIndexSet(),Array[DataValueCursor](subnetwork_cur.values())):
            ne_list.append(subnetwork_cur['NodeName'].CurrentValue)
    if len(ne_list) == 0:
		ne_list.append('')
    node_list = ','.join("'{}'".format(i) for i in ne_list)
       
    return node_list   

def getTopologyTableData(nodeType, dataS):	
    topologyTableName = 'Modified Topology Data'
    topologyDataTable = Document.Data.Tables[topologyTableName]
    topologyDataTableCur = create_cursor(topologyDataTable)

    selectedNodeType = topologyDataTable.Select("[Node]= '" + nodeType + "'" )

    for node in topologyDataTable.GetRows(selectedNodeType.AsIndexSet(), Array[DataValueCursor](topologyDataTableCur.values())):
        serverName = topologyDataTableCur['DataSourceName'].CurrentValue
        if serverName == dataS:
            tableName = topologyDataTableCur['Topology Table'].CurrentValue
            FDNName = topologyDataTableCur['FDN Key'].CurrentValue
            KeyName = topologyDataTableCur['Key'].CurrentValue
            print tableName
    return tableName + ',' + FDNName + ',' + KeyName


def runQuery(sql,EniqName):
    try:
        connString = "DSN=" + EniqName
        connection = OdbcConnection(connString)

        dataSet = DataSet()
        start = time.time()
        connection.Open()
        adaptor = OdbcDataAdapter(sql, connection)
        dataSet = DataSet()
        adaptor.Fill(dataSet)
        connection.Close()
        end = time.time()
        print "overall:" + str(end - start)

        return dataSet
    except Exception as e:
        print(e)
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))


def wildcardQuery(ne_collection):
    active_collection = node_collections_data_table.Select("[CollectionName] ='" + ne_collection + "'")
    for nodes in node_collections_data_table.GetRows(active_collection.AsIndexSet(), Array[DataValueCursor](node_collection_cur.values())):
        Collection_type=node_collection_cur['CollectionType'].CurrentValue
        SystemArea=node_collection_cur['SystemArea'].CurrentValue
        NodeType=node_collection_cur['NodeType'].CurrentValue
        EniqName=node_collection_cur['EniqName'].CurrentValue
        WildcardExpression=node_collection_cur['WildCardDefinition'].CurrentValue

    if '_' in WildcardExpression:
        wildcardExpressionFinal = WildcardExpression.replace("_","[_]")
    else:
        wildcardExpressionFinal = WildcardExpression
		 
    alldata = getTopologyTableData(NodeType,EniqName).split(',')
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
    where
	{7}""".format(KeyName,FDNName,NodeType,SystemArea,ne_collection, WildcardExpression.replace("'", "''"),tableName, wildcardExpressionFinal)
    dataSet = runQuery(sql,EniqName)
    nodenames = [row[0] for row in dataSet.Tables[0].Rows]
    return nodenames												

@timeit
def replace_collection(alarm_query, collection_name):
    """ replaces the @node_collection with the list of nodes from collection tables """
    ne_list = []
    collection_table = 'Alarm Collections'
    filter_condition = """[CollectionName] = '{collection_name}'""".format(collection_name=collection_name)
    collection_data_table, collection_cursor, collection_index = get_spotfire_dt(collection_table, filter_condition)

    for nodes in collection_data_table.GetRows(collection_index, Array[DataValueCursor](collection_cursor.values())):
        if collection_cursor['CollectionType'].CurrentValue == 'Single Node':
            node_list = "'" + collection_name + "'"
            return alarm_query.replace('@node_collection', node_list)
        elif collection_cursor['CollectionType'].CurrentValue == 'Subnetwork':
            node_list = FetchNodesForSubnetwork(collection_name)
            return alarm_query.replace('@node_collection', node_list)
        elif (collection_cursor['TypeOfCollection'].CurrentValue=='Dynamic'):
            ne_list=wildcardQuery(collection_name)              
        else:
            ne_list.append(collection_cursor['NodeName'].CurrentValue)
            

    node_list = ','.join("'{}'".format(i) for i in ne_list)

    return alarm_query.replace('@node_collection', node_list)


@timeit
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

    # Convert any columns to the appropriate formatter
    for column in Document.Data.Tables[table_name].Columns:
        formatter = DATETIME_FORMATTER
        if column.Name == 'DATE_ID':
            formatter = DATE_FORMATTER
               
        if column.Properties.DataType.ToString() in ['DateTime','Date']:
            column.Properties.DataType == DataType.Date
            column.Properties.Formatter = formatter


@timeit
def run_query(alarms_queries):
    """ run a a set of alarm queries (these are tuples)"""

    try:
        table_name = alarms_queries[0] 
        sql = alarms_queries[1]
        eniq_name = alarms_queries[2]
        CONN_STRING = "DSN=" + eniq_name
        connection = OdbcConnection(CONN_STRING)
        connection.Open()
        data_set = DataSet()
        adaptor = OdbcDataAdapter(sql, connection)
        adaptor.Fill(data_set)

        logger.info("Created alarm table for: " + table_name)
        logger.info("Number of rows returned: "+str(data_set.Tables[0].Rows.Count))

        connection.Close()      
        return data_set, table_name

    except Exception as e:
        logger.error("Failed to pull data for: " + table_name)
        logger.error(e.message)
        return '', table_name


@timeit       
def add_calc_columns(alarm_name):
    """ add the calulated columns from the data table for the alarm """
    alarm_columns = {}
    alarm_column_data_type ={}
    alarm_table = 'Alarm Formulas'
    filter_condition = """[AlarmName] = '{alarm_name}'""".format(alarm_name=alarm_name)
    alarm_formula_dt, alarm_formula_cursor, alarm_formula_index = get_spotfire_dt(alarm_table, filter_condition)

    # for given alarm name loop through tblAlarmFormulas and add to dict by column name and formula
    for selectedmeasure in alarm_formula_dt.GetRows(alarm_formula_index, Array[DataValueCursor](alarm_formula_cursor.values())):
        formula_column_name = alarm_formula_cursor['AlarmColumnName'].CurrentValue
        formula_value = alarm_formula_cursor['AlarmColumnFormula'].CurrentValue
        formula_data_type = alarm_formula_cursor['AlarmColumnDataType'].CurrentValue
      
        alarm_columns[formula_column_name] = formula_value
        alarm_column_data_type[formula_column_name] = formula_data_type
   
    # add calculated cols to table (same as alarm_name)
    alarm_table_cols = Document.Data.Tables[alarm_name].Columns

    # add all the col names first with datatype place holders. 
    # then add formulas (this avoids error with columns not added in order)
    for column_name, column_formula in alarm_columns.items():
        try:
            column_data_type = alarm_column_data_type[column_name]
            placeholder_expression = PLACEHOLDER_EXPRESSIONS[column_data_type]
        except Exception as e:
            placeholder_expression = "'0'"
        alarm_table_cols.AddCalculatedColumn(column_name, placeholder_expression)

    try:
        for column_name, column_formula in alarm_columns.items():
            calc_col = Document.Data.Tables[alarm_name].Columns[column_name].As[CalculatedColumn]()
            calc_col.Expression = column_formula
    except Exception as e:
        logger.error("""Error updating calculated column expression for: alarm_name:{alarm_name}, column_name:{column_name}, column_formula:{column_formula} """.format(alarm_name=alarm_name,column_name=column_name,column_formula=column_formula))
        logger.error(e.message)


def get_spotfire_dt(source_data_table_name, filtercondition):
    """ for a given spotfire table return the tablename, cursor and indexset """

    source_data_table = Document.Data.Tables[source_data_table_name]
    source_cur = create_cursor(source_data_table)
    
    if filtercondition != 'None':
        index_filter = Document.Data.Tables[source_data_table_name].Select(filtercondition)
        index_set = index_filter.AsIndexSet()

        return source_data_table, source_cur, index_set

    return source_data_table, source_cur


def remove_table_extension(table_name):
    """Removes PM data table sufix 

    Arguments:
        table_name {string} -- PM Data table name
    Returns:
        table_name {string} -- PM Data table name with removed sufix
    """

    return table_name.replace('_RAW', '').replace('_DELTA', '').replace('_DAY', '')


@timeit
def main():
    # main function to loop through alarms, check if they can be ran, and then fetch data for them

    alarm_def_dt_name = 'Alarm Definitions'
    alarm_def_dt, alarm_def_cursor = get_spotfire_dt(alarm_def_dt_name,'None')

    alarms_queries = []
    date_value = datetime.now()
    failed_to_run_any= False

    logger.info("Retriving data from ENIQ for active alarms...")
    for selectedmeasure in alarm_def_dt.GetRows(Array[DataValueCursor](alarm_def_cursor.values())):
        try:
            alarm_name = alarm_def_cursor['AlarmName'].CurrentValue
            eniq_name = alarm_def_cursor['EniqName'].CurrentValue
            if check_alarm_can_run(alarm_name):
                collection_name = alarm_def_cursor['NECollection'].CurrentValue
                tablename = remove_table_extension(alarm_def_cursor['TableName'].CurrentValue)

                # if there is a multi table kpi, use only the first table to check against log load table etc.
                tablenames = tablename.split(',')
                filtercondition = ""
                for table in tablename.split(','):	
                    if 'ERBS' in table:
                        g2_table_name = table.replace('ERBS_','ERBSG2_')
                        tablenames.append(g2_table_name)
                for table in tablenames:
                    if table == tablenames[-1]:
                        filtercondition += ("""[typename] = '{table}'""".format(table = table))
                    else:
                        filtercondition += ("""[typename] = '{table}'""".format(table = table) + " OR ")
                
                alarm_query = alarm_def_cursor['AlarmQuery'].CurrentValue

                log_load_tablename = 'Log_LoadStatus_'+eniq_name
                log_load_dt, log_load_cursor, log_load_index = get_spotfire_dt(log_load_tablename, filtercondition)

                timestamps = []
                for _ in log_load_dt.GetRows(log_load_index, Array[DataValueCursor](log_load_cursor.values())):
                    if log_load_cursor['datatime'].CurrentValue:
                        timestamps.append(log_load_cursor['datatime'].CurrentValue)
                date_value = max(dt for dt in timestamps)
                query = replace_date_time(alarm_query, date_value)
                query = replace_collection(query, collection_name)
                alarms_queries.append((alarm_name, query, eniq_name))

        except Exception as e:
            logger.error("Failed to get format query for alarm: " + alarm_name)
            logger.error(e.message)

    try:
        if alarms_queries:
            dataset_list_with_None = []
            dataset_list = []
            dataset_list_with_None = POOL.map(run_query, alarms_queries)
            POOL.terminate()
            POOL.join()
            dataset_list = filter(lambda x: x != None, dataset_list_with_None)
            dataset_list_with_None = []

            for ds, alarm_name in dataset_list:
                try:
                    column_names, column_types = get_column_names_and_types(ds)
                    text_data = generate_text_data(ds, column_names)
                    create_data_table(alarm_name, text_data, column_names, column_types)
                    logger.info("Alarm data retrieved for: " + alarm_name)
                    
                    add_calc_columns(alarm_name)
                    
                except Exception as e:
                    logger.error("Failed to create data table and add columns for alarm: " + alarm_name)
                    logger.error(e.message)
                    failed_to_run_any = True

            if failed_to_run_any:
                logger.info("Some alarms did not generate properly. Successful alarms will continue.") 
            else:
                logger.info("Alarm data retrived from ENIQ.")

            dataset_list = []

            # run the next script to create the consolidated table for ENM/ENIQ
            Document.Properties["RunCreateENMTable"] = DateTime.UtcNow    
    except Exception as e:
        logger.error("Failed to run queries and generate tables.")
        logger.error(e.message)
        tables_to_keep = ["Alarm Definitions","empty_data_table", "vwEniqEnm"]            
        clear_tables(tables_to_keep)
        dataset_list_with_None = []
        dataset_list = []
        POOL.terminate()
        POOL.join()

node_collections_data_table_name = 'Alarm Collections'
node_collections_data_table = Document.Data.Tables[node_collections_data_table_name]
node_collection_cur = create_cursor(node_collections_data_table)													  
        
main()
