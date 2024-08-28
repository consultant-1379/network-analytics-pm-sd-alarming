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
# Name    : ImportIntialData.py
# Date    : 10/1/2022
# Revision: 2.0
# Purpose :
#
# Usage   : PM Alarming
#

import clr
clr.AddReference('System.Data')
import time
import ast 
import re
from System import Array
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcDataAdapter
from System import DateTime
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System.Collections.Generic import Dictionary
from System import Threading
from multiprocessing.pool import ThreadPool
from System import Array, Byte
from System.Text import UTF8Encoding
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from System.Collections.Generic import List
from Spotfire.Dxp.Data import DataColumn, DataColumnType
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
import logging
from collections import defaultdict
from Spotfire.Dxp.Data import DataType
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings																																																								   
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data.Formatters import *

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
import re
from Spotfire.Dxp.Framework.Library import *
from Spotfire.Dxp.Data.Import import SbdfLibraryDataSource


# global vars
POOL = ThreadPool(5)
ALARM_SCHEDULE = Document.Properties['AlarmSchedule']
PMA_AGGREGATION_FIELD_VALUE = {
    '5':'5',
    '15':'None',
    '60':'1 Hour',
    '1440':'1 Day'
}
ALARM_AGG_VALUE = PMA_AGGREGATION_FIELD_VALUE[ALARM_SCHEDULE]

flag_subnetCheck = False

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

custom_date_format = 'yyyy-MM-dd HH:mm:ss'
FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
FORMATTER.FormatString = custom_date_format

logger = logging.getLogger('root')
current_script = 'ImportIntialData'
logging.basicConfig(
    format="""%(asctime)s|%(levelname)s|{current_schedule}|{current_script}|%(message)s""".format(current_schedule=ALARM_SCHEDULE,current_script=current_script),
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)


def timeit(method):
    """ timing decorator for functions """
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


def clear_tables(table_names):
    for table in Document.Data.Tables:
        if table.Name not in table_names:
            Document.Data.Tables.Remove(Document.Data.Tables[table.Name])


def getTheTableName(eniq):
    try:
        sql = "SELECT TOP 1 * FROM DC_Z_ALARM_NETAN_RAW"
        dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + eniq, sql)
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        dt = Document.Data.Tables.Add('temp', dataTableDataSource)
        Document.Data.Tables.Remove('temp')
        return 'DC_Z_ALARM_NETAN_RAW'
    except Exception as e:
        #print e.message
        logger.info('DC_Z_ALARM_NETAN_RAW is not present in the DB')
        return 'DC_Z_ALARM_INFO_RAW'						  
def create_cursor(table):
    """Create cursors for a given table, these are used to loop through columns"""
    
    curs_list = []
    col_list = []

    for column in table.Columns:
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name], FORMATTER))
            col_list.append(table.Columns[column.Name].ToString())
        else:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name]))
            col_list.append(table.Columns[column.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict


def get_spotfire_dt(source_data_table_name, filtercondition):
    """ for a given spotfire table return the tablename, cursor and indexset """

    source_data_table = Document.Data.Tables[source_data_table_name]
    source_cur = create_cursor(source_data_table)
    
    if filtercondition != 'None':
        index_filter = Document.Data.Tables[source_data_table_name].Select(filtercondition)
        index_set = index_filter.AsIndexSet()

        return source_data_table, source_cur, index_set

    return source_data_table, source_cur


def generate_text_data(data_set, column_names):
    """ generator function - yields rows of data for a dataset object loaded with sql tables """
    
    curr_row = ""
    for row in data_set.Tables[0].Rows:
        curr_row = []
        for col in column_names:
            curr_row.append(str(row[col]).replace('\r\n',' '))
        yield "%s\r\n" % ('|'.join(curr_row))


def get_list_from_column(column):
    """ for a given column, return a list of values for a column - specific to alarm definitions"""

    alarm_def_dt_name = 'Alarm Definitions'
    alarm_filtercondition = "[AlarmState] = 'Active'"
    alarm_def_dt, alarm_def_cursor, alarm_index = get_spotfire_dt(alarm_def_dt_name, alarm_filtercondition)

    return ["'{value}'".format(value=alarm_def_cursor[column].CurrentValue) for selectedmeasure in alarm_def_dt.GetRows(alarm_index, Array[DataValueCursor](alarm_def_cursor.values()))]


def run_query(alarms_queries):
    """ run a a set of alarm queries (these are tuples)"""
    try:
        table_name = alarms_queries[0] 
        sql = alarms_queries[1]
        connection_string = alarms_queries[2]
        connection = OdbcConnection(connection_string)
        connection.Open()
        data_set = DataSet()
        adaptor = OdbcDataAdapter(sql, connection)
        adaptor.Fill(data_set)
        connection.Close()
        logger.info("SQL query succesful for: " + table_name)
        return data_set, table_name
    except Exception as e:
        logger.error("Failed to pull data for: " + table_name)
        logger.error(e.message)
        raise


def get_column_names_and_types(data_set):
    """ for a dataset object, return the column types(refering to a type dict) and names"""

    column_types = []
    column_names = []

    for column in data_set.Tables[0].Columns:
        column_names.append(column.ColumnName)
        col_type = column.DataType.Name.ToString()
        if (ALL_COLUMN_TYPES.get(col_type)):
            column_types.append(ALL_COLUMN_TYPES[col_type])
        else:
            column_types.append(DataType.String)

    return column_names, column_types  


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

    # for 5 min nodes, push all results of datasets to one table and keep adding
    # using same name for Log_load as other workers to keep code consistent throughout
    if "5minlog|" in table_name:
        eniq_name = table_name.split("'", 1)[1]
        table_name = 'Log_LoadStatus_'+eniq_name
        if Document.Data.Tables.Contains(table_name):
            data_table = Document.Data.Tables[table_name]
            settings = AddRowsSettings(data_table, fs)
            data_table.AddRows(fs, settings)
        else:
            Document.Data.Tables.Add(table_name, fs)
    else:
        if Document.Data.Tables.Contains(table_name):
            Document.Data.Tables.Remove(Document.Data.Tables[table_name])

        Document.Data.Tables.Add(table_name, fs)

    # Convert any columns to the appropriate formatter
    for column in Document.Data.Tables[table_name].Columns:
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            column.Properties.DataType == DataType.Date
            column.Properties.Formatter = FORMATTER


def generate_and_create_table_data(queries_dict, connection_string=None, ds ="Netan"):
    """ loop through a dictionary of queries and run using threads """

    alarms_queries = []

    if ds == "Eniq":
        for eniq, queries in queries_dict.items():
            for data_table_name, query in queries.items():
                connection_string = "DSN=" + eniq + ";Pooling=true;Max Pool Size=20;Enlist=true;FetchArraySize=100000;"
                alarms_queries.append((data_table_name, query, connection_string))
    else:
        for data_table_name, query in queries_dict.items():
            alarms_queries.append((data_table_name, query, connection_string))
  
    datasetlist = []
    datasetlist_with_none = []
   
    try:

        datasetlist_with_none = POOL.map(run_query, alarms_queries)
        datasetlist = filter(lambda x: x != None, datasetlist_with_none)
        datasetlist_with_none = []
          
        # need to remove log_load for 5 min tables the first time, as in the loop it will be adding rows to the data table and cant remove there
        if ALARM_SCHEDULE == '5':
            if Document.Data.Tables.Contains('Log_LoadStatus'):
                Document.Data.Tables.Remove(Document.Data.Tables['Log_LoadStatus'])
        
        
        for dataset, table_name in datasetlist:
            
            column_names, column_types = get_column_names_and_types(dataset)
            text_data = generate_text_data(dataset, column_names)
            create_data_table(table_name, text_data, column_names, column_types)
        
        datasetlist = []
      
    except Exception as e:
        datasetlist = []
        datasetlist_with_none = []
        logger.error("Failed to generate table. Stopping process.")
        logger.error(e.message)
        raise


def netan_db_queries():
    """ returns a dictonary of queries to run from the netan db. """

    add_where_clause = ""
    if ALARM_SCHEDULE == '5':
        key_field = '"PeriodDuration"'
        add_where_clause = """ and "Aggregation" = 'None'"""
    elif ALARM_SCHEDULE == '60':
        key_field = '"Aggregation"'
        add_where_clause = ''
    else:
        key_field = '"Aggregation"'
        add_where_clause = """ and "PeriodDuration" != '5'"""

    query_alarm_defintions = """
    SELECT * FROM "vwAlarmDefinitions" WHERE "AlarmState" = 'Active' and "AlarmQuery" <> '' and {key_field} = '{alarm_schedule}' {add_where_clause}
    """.format(key_field=key_field, alarm_schedule=ALARM_AGG_VALUE, add_where_clause=add_where_clause)


    query_alarm_formulas = """
    SELECT af.* FROM "tblAlarmFormulas" af, "tblAlarmDefinitions" ad WHERE ad."AlarmID" = af."AlarmID" AND ad."AlarmState" = 'Active' and ad.{key_field} = '{alarm_schedule}' {add_where_clause}
    """.format(key_field=key_field, alarm_schedule=ALARM_AGG_VALUE, add_where_clause=add_where_clause)

    query_alarm_collection = """
        SELECT
        c."CollectionName",
        c."CollectionType",
        c."NodeType",
        c."SystemArea",
        e."EniqName",
        c."TypeOfCollection",
        n."WildCardDefinition",
        n."NodeName"
        FROM
            "tblCollection" c
        LEFT JOIN "tblNode" n ON
            c."CollectionID" = n."CollectionID"
        LEFT JOIN "tblEniqDS" e ON
            c."EniqID" = e."EniqID"
        WHERE
            c."CollectionID" IN (
            SELECT
                "CollectionID"
            FROM
                "tblAlarmDefinitions"
            WHERE
                "AlarmState" = 'Active'
                AND {key_field} = '{alarm_schedule}'
                {add_where_clause}
                )
    """.format(key_field=key_field, alarm_schedule=ALARM_AGG_VALUE, add_where_clause=add_where_clause)
    netan_db_queries_dict = {
       'Alarm Definitions': query_alarm_defintions,
       'Alarm Formulas': query_alarm_formulas,
       'Alarm Collections': query_alarm_collection
    }

    return netan_db_queries_dict


def remove_table_extension(table_name):
    """Removes PM data table sufix 

    Arguments:
        table_name {string} -- PM Data table name
    Returns:
        table_name {string} -- PM Data table name with removed sufix
    """

    return table_name.replace('_RAW', '').replace('_DELTA', '').replace('_DAY', '')



def eniq_groups(alarm_name, eniq_name):
    eniq_info = {}
    for alarm, eniq in zip(alarm_name, eniq_name):
        if eniq in eniq_info:
            eniq_info[eniq].append(alarm)
        else:
            eniq_info[eniq] = [alarm]
    
    return eniq_info


        
  
def eniq_db_queries():
    """ returns a dictionary of eniq queries to run """

    # get list of tables to limit the query by
    list_of_tables = list(set(get_list_from_column('TableName')))
    for table in list(set(get_list_from_column('TableName'))):
        if 'ERBS' in table:
            g2_table_name = table.replace('ERBS_','ERBSG2_')
            list_of_tables.append(g2_table_name)

    updated_list_of_tables = set()
    for table in list_of_tables:
        for tab in table.split(","):
            updated_list_of_tables.add("'" + tab.replace("'","") + "'")
    updated_list_of_tables = list(updated_list_of_tables)

    list_of_tables_clean = [remove_table_extension(table) for table in updated_list_of_tables]
    query_log_load = ''
  
    
    eniq_db_info = {}
    alarm_names = get_list_from_column('AlarmName')
    eniq_names = get_list_from_column('EniqName')
    eniq_mapping = eniq_groups(alarm_names, eniq_names)
    per_eniq_queries = {}



    #============================
    # 1. Log_LoadStatus queries
    #============================

    # query for day alarms
    for eniq, alarms in eniq_mapping.items():
        eniq_queries_dict = {}
        eniq = eniq.replace("'", "")
        query_log_load_staus_day = """
        SELECT
            upper(typename) AS  typename,
            min(datatime) as "datatime",
            dateadd(day,-1, convert(date,getdate()) ) as "previous_datatime",
            '{eniqs}' as 'EniqDs'
        FROM
            (
            SELECT
                max(DATE_ID) as "datatime",
                typename
                FROM
                    LOG_AggregationStatus
                where
                    status = 'Aggregated'
                    and TIMELEVEL = 'DAY'
                    and typename in ({tables})
                    and ROWCOUNT > 0
                group by
                    typename) as sub
                    GROUP BY typename
                """.format(eniqs=eniq, tables=','.join(list_of_tables_clean))
    
        # query for 15 min, 1 hour alarms
        query_log_load_staus_raw = """
        SELECT
            upper(typename) AS "typename",
            min(datatime) as "datatime",
            DATEADD(mi,-{schedule},datatime) as "previous_datatime",
            '{eniqs}' as 'EniqDs'
        FROM(
            SELECT
                max(datatime) as "datatime",
                typename
            FROM
                LOG_LOADSTATUS
            where
                status = 'LOADED'
                and ROWCOUNT > 0
                and typename in ({tables})
            group by
                typename) AS sub
                GROUP BY typename
            """.format(eniqs=eniq, schedule=ALARM_SCHEDULE,tables=','.join(list_of_tables_clean))
    
        # depending on the alarm, need to trigger different queries from eniq
        if ALARM_SCHEDULE in ['15', '60']:
            query_log_load = query_log_load_staus_raw
        elif ALARM_SCHEDULE == '1440':
            query_log_load = query_log_load_staus_day
         
        if query_log_load:
            eniq_queries_dict['Log_LoadStatus_'+eniq]= query_log_load
    
        # 5 min node queries. Formatted the same as other log load status queries
        if ALARM_SCHEDULE == '5':
            for table in updated_list_of_tables:
                table = table.replace("'", "")
                table_without_suffix = remove_table_extension(table)
    
                query = """SELECT UPPER('{table_without_suffix}') AS typename,
                        MAX(DATETIME_ID) as "datatime",
                        DATEADD(mi,-5,datatime) as "previous_datatime", '{eniqs}' as 'EniqDs'
                        from {table} """.format(eniqs=eniq, table_without_suffix=table_without_suffix, table=table)
    
                eniq_queries_dict["5minlog|" + table + "'" + eniq] = query 

    #==========================
    # 2. DC_Z_ALARM_INFO query
    #==========================
        eniq = eniq.replace("'", "")
        tmp = {}
        dc_z_table = getTheTableName(eniq)
        alarm_info_table_query = """
        select AlarmName,MAX(DATETIME_ID) as Max_DateTime,MAX(DATE_ID) as Max_Date, '{eniqs}' as 'EniqDs' FROM {table} WHERE ReportTitle = 'PM Alarm' and AlarmName in ({alarm_names}) GROUP BY AlarmName
        """.format(eniqs=eniq, alarm_names=','.join(alarms),table = dc_z_table)
        tmp[dc_z_table+'_'+eniq] = alarm_info_table_query
        per_eniq_queries[eniq] = tmp
        per_eniq_queries[eniq].update(eniq_queries_dict)


    return per_eniq_queries

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
        
def fetchSubnetworks(topologyFilter): 
    TopologyRows =[]
    topologyTable = Document.Data.Tables['Topology Data']
    topologySelect = ''
    '''node_types = ''
    eniq_present = ''
    for node in nodeType:
        if node_types == '':
            node_types = '[Node] = "' + node + '"'
        else:
            node_types = node_types + ' or [Node] = "' + node + '"'
    for eniq in eniq_servers:
        if eniq_present == '':
            eniq_present = '[DataSourceName] = "' + eniq + '"'
        else:
            eniq_present = eniq_present + ' or [DataSourceName] = "' + eniq + '"'''
    for topo in topologyFilter:
        if topologySelect == '':
            topologySelect = topo
        else:
            topologySelect = topologySelect + ' or ' + topologySelect       
    filter_topology = topologyTable.Select(topologySelect)
    cursor_topology = create_cursor(topologyTable)
    for row in topologyTable.GetRows(filter_topology.AsIndexSet(), Array[DataValueCursor](cursor_topology.values())):
        rowtemp = []
        for col in topologyColumns:
            rowtemp.append(cursor_topology[col].CurrentValue)
        TopologyRows.append(';'.join(rowtemp))   
        
    ModifiedtopologyTable = 'Modified Topology Data'            
    if Document.Data.Tables.Contains(ModifiedtopologyTable):
        Document.Data.Tables.Remove(ModifiedtopologyTable)
           
    topologyStream = MemoryStream()
    topologyCsvWriter = StreamWriter(topologyStream)
    topologyCsvWriter.WriteLine(';'.join(topologyColumns) + '\r\n')
    topologyCsvWriter.Write('\r\n'.join(TopologyRows))
    topologyCsvWriter.Flush()
    topologyStream.Seek(0, SeekOrigin.Begin)
    createTable(ModifiedtopologyTable, topologyStream)


def execute():
    
    topologyDataTable = 'Topology Data'
    '''if Document.Data.Tables.Contains(topologyDataTable):
			Document.Data.Tables.Remove(topologyDataTable)'''

    
	   
		
    topologyStream = MemoryStream()
    topologyCsvWriter = StreamWriter(topologyStream)
    topologyCsvWriter.WriteLine(';'.join(topologyColumns) + '\r\n')
    topologyCsvWriter.Flush()
    createTable(topologyDataTable, topologyStream)
    for eniq in getConnectedServers(): 
		manager = Application.GetService[LibraryManager]()
		libraryPathsTopology = ["/Ericsson Library/General/PM Data/PM-Data/Analysis/Topology Data","/Ericsson Library/General/PM Data/PM-Data/Analysis/Service Topology Data"]
		for libraryPath in libraryPathsTopology:
			(found, item) = manager.TryGetItem(libraryPath,LibraryItemType.SbdfDataFile,LibraryItemRetrievalOption.IncludePath)
			if found:
				ds = SbdfLibraryDataSource(item)
				rowsettings = AddRowsSettings(Document.Data.Tables[topologyDataTable],ds,"DataSourceName",eniq)
				Document.Data.Tables[topologyDataTable].AddRows(ds,rowsettings)



def findTheRowsWithSubnetwork(table):
    filter_alarm = table.Select('[SingleOrCollection] = "Subnetwork" or [SingleOrCollection] = "Collection"')
    alarm_def_cursor = create_cursor(table)
    topologyFilter = []
    #eniq_servers = []
    for row in table.GetRows(filter_alarm.AsIndexSet(), Array[DataValueCursor](alarm_def_cursor.values())):
        topologyFilter.append('([Node] = "' + alarm_def_cursor['NodeType'].CurrentValue +'" and [DataSourceName] = "' + alarm_def_cursor['EniqName'].CurrentValue + '")')
    if len(topologyFilter) == 0:		
		flag_subnetCheck = True	
		return flag_subnetCheck	
    else: 
		flag_subnetCheck = False
		fetchSubnetworks(list(set(topologyFilter)))
		return flag_subnetCheck	

SubnetworkColumn = '''NodeName
FDN
NodeType
SystemArea
DataSourceName'''.split('\n')

def createSubnetworkTable(table,modifiedCur):
    connectedServer = allConnectedServers(table,modifiedCur)
    subnetworkRows = []
    for server in connectedServer:
        sql = ''
        filter_modified = table.Select('[DataSourceName] = "' + server + '"')
        for row in table.GetRows(filter_modified.AsIndexSet(), Array[DataValueCursor](modifiedCur.values())):
            if sql == '':
                sql = """
                    SELECT DISTINCT            
                        {0}  as NodeName, 
                        {1}  as FDN,  
                        '{2}' as NodeType, 
                        '{3}' as SystemArea,
                        '{4}' as DataSourceName
                    FROM
                        {5}
                    """.format(modifiedCur['Key'].CurrentValue,modifiedCur['FDN Key'].CurrentValue,modifiedCur['Node'].CurrentValue,modifiedCur['System Area'].CurrentValue,modifiedCur['DataSourceName'].CurrentValue, modifiedCur['Topology Table'].CurrentValue)
            else:
                sql = sql + """
                    union all
                    SELECT DISTINCT            
                        {0}  as NodeName, 
                        {1}  as FDN,  
                        '{2}' as NodeType, 
                        '{3}' as SystemArea,
                        '{4}' as DataSourceName
                    FROM
                        {5}
                    """.format(modifiedCur['Key'].CurrentValue,modifiedCur['FDN Key'].CurrentValue,modifiedCur['Node'].CurrentValue,modifiedCur['System Area'].CurrentValue,modifiedCur['DataSourceName'].CurrentValue, modifiedCur['Topology Table'].CurrentValue)  
                  
        db_settings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + server, sql)
        ds = DatabaseDataSource(db_settings)
        new_data_table = Document.Data.Tables.Add('temp',ds)
        temp_cur = create_cursor(Document.Data.Tables['temp'])
        for row in Document.Data.Tables['temp'].GetRows(Array[DataValueCursor](temp_cur.values())):
            col_temp = []
            for col in SubnetworkColumn:
                col_temp.append(temp_cur[col].CurrentValue)
            subnetworkRows.append(';'.join(col_temp))
            
        if Document.Data.Tables.Contains('temp'): 
            Document.Data.Tables.Remove('temp')
            
    SubnetworkCal = subNetworkCollectionTableName            
    if Document.Data.Tables.Contains(SubnetworkCal):
        Document.Data.Tables.Remove(SubnetworkCal)
        
    subnetworkStream = MemoryStream()
    subnetworkCsvWriter = StreamWriter(subnetworkStream)
    subnetworkCsvWriter.WriteLine(';'.join(SubnetworkColumn) + '\r\n')
    subnetworkCsvWriter.Write('\r\n'.join(subnetworkRows))
    subnetworkCsvWriter.Flush()
    subnetworkStream.Seek(0, SeekOrigin.Begin)
    createTable(SubnetworkCal, subnetworkStream)


def getConnectedServers():
    ServersConnected = []
    AlarmDefinition = Document.Data.Tables['Alarm Definitions']
    alarm_def_cur = create_cursor(AlarmDefinition)
    for row in AlarmDefinition.GetRows(Array[DataValueCursor](alarm_def_cur.values())):
        ServersConnected.append(alarm_def_cur['EniqName'].CurrentValue)
    return list(set(ServersConnected))            
           
def allConnectedServers(table,modifiedCur):
    ServersConnected = []
    for row in table.GetRows(Array[DataValueCursor](modifiedCur.values())):
        ServersConnected.append(modifiedCur['DataSourceName'].CurrentValue)
    return list(set(ServersConnected))


def removeCalculatedColumns(dataTable):
    columnCollection = dataTable.Columns
    columnsToRemove = List[DataColumn]()
    for column in columnCollection:
        # print('Column: ', column.Name, column.Properties.ColumnType)
        if column.Properties.ColumnType == DataColumnType.Calculated:
            # print('Column: ', column.Name, column.Properties.ColumnType)
            columnsToRemove.Add(column)
    columnCollection.Remove(columnsToRemove)

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
             if dataTableCursor['SubnetworkName'].CurrentValue == '(Empty)':
                 rowsToFilter.AddIndex(measureMappingRow.Index)
         
         datatable.RemoveRows(RowSelection(rowsToFilter))


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

       
def getNodesForSubnetwork():
    modifiedTable = Document.Data.Tables['Modified Topology Data']
    modifiedCur = create_cursor(modifiedTable)   
    createSubnetworkTable(modifiedTable,modifiedCur)
    if Document.Data.Tables.Contains(subNetworkCollectionTableName):
        buildSubNetworkCollections(Document.Data.Tables[subNetworkCollectionTableName])
        addSubNetworkCollections(Document.Data.Tables[subNetworkCollectionTableName])
        deleteInvalidRows()

        

def main():
    """ main function for calling all required queries """

    CONN_STRING_NETAN = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
    try:
		logger.info('Starting scheduled run of worker file...')
		logger.info('Starting import of tables...')
		generate_and_create_table_data(netan_db_queries(), connection_string = CONN_STRING_NETAN, ds="Netan")
		generate_and_create_table_data(eniq_db_queries(), ds="Eniq")
		if Document.Data.Tables.Contains('Alarm Definitions'):
			execute()			
			flag_subnetCheck = findTheRowsWithSubnetwork(Document.Data.Tables['Alarm Definitions'])
			if flag_subnetCheck != True:
				getNodesForSubnetwork()			
		POOL.terminate()
		POOL.join()
		logger.info('Succesfully generated all intial tables required.')

		#trigger next script to check if alarms are ready to be alarmed
		Document.Properties["RunCheckIfReadyToAlarm"] = DateTime.UtcNow

    except Exception as e:
        print e.message
        logger.error("Failed to generate all tables. Stopping process.")
        POOL.terminate()
        POOL.join()
        tables_to_keep = ["Alarm Definitions","empty_data_table", "vwEniqEnm"]            
        clear_tables(tables_to_keep)


topologyColumns = '''Topology Table
Node
System Area
Key
DataSourceName
FDN Key'''.split('\n')


subNetworkCollectionTableName = 'FDN SubNetwork List From ENIQ'
nodeCollectionTableName = 'SubNetwork List'

if Threading.Thread.CurrentPrincipal.Identity.Name == 'scheduledupdates@SPOTFIRESYSTEM':
	main()
