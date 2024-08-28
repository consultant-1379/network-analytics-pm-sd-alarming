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
# Name    : LoadMarkedAlarm.py
# Date    : 09/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarming
#

import clr
clr.AddReference('System.Data')
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data import *
from System.Data.Odbc import OdbcConnection
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from System import Array
from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters
from Spotfire.Dxp.Data import DataPropertyClass
from Spotfire.Dxp.Framework.ApplicationModel import *
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcDataAdapter
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
import time
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

# global vars
EniqDataSource = ""

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)

notify = Application.GetService[NotificationService]()


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
    conn_string_netan = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)

class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'MeasureName'
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
    SystemArea = 'SystemArea'
    LookBackVal = 'LookBackVal'
    LookBackUnit = 'LookBackUnit'
    DataRangeVal = 'DataRangeVal'
    DataRangeUnit = 'DataRangeUnit'
    PeriodDuration = 'PeriodDuration'
    TableName = 'TableName'
    EniqName = 'EniqName'


def create_table(data_table_name, text_data):
    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine("node;NodeType;SystemArea\r\n")
    writer.Flush()

    for line in text_data:
        writer.WriteLine(line)
    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = ";"
    settings.AddColumnNameRow(0)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(data_table_name):
        data_table = Document.Data.Tables[data_table_name]
        data_table.RemoveRows(RowSelection(IndexSet(data_table.RowCount,True)))
        settings = AddRowsSettings(data_table, fs)
        data_table.AddRows(fs, settings)
    else:
        data_table = Document.Data.Tables.Add(data_table_name, fs)

def create_cursor(e_table):
    curs_list = []
    col_list = []

    for e_column in e_table.Columns:
        curs_list.append(DataValueCursor.CreateFormatted(e_table.Columns[e_column.Name]))
        col_list.append(e_table.Columns[e_column.Name].ToString())
    cusr_dict=dict(zip(col_list,curs_list))

    return cusr_dict


def get_topology_table_data(node_type, system_area):

    topology_table_name = 'Topology Data'
    topology_data_table = Document.Data.Tables[topology_table_name]
    topology_data_table_cur = create_cursor(topology_data_table)

    selected_node_type = topology_data_table.Select("[Node]= '" + node_type + "'")

    for node in topology_data_table.GetRows(selected_node_type.AsIndexSet(), Array[DataValueCursor](topology_data_table_cur.values())):
        table_name = topology_data_table_cur['Topology Table'].CurrentValue
        key_name = topology_data_table_cur['Key'].CurrentValue

    return table_name + ',' + key_name

def run_query(sql):

    conn_string = "DSN=" + EniqDataSource
    connection = OdbcConnection(conn_string)

    data_set = DataSet()
    start = time.time()
    connection.Open()
    adaptor = OdbcDataAdapter(sql, connection)
    adaptor.Fill(data_set)
    connection.Close()
    end = time.time()

    return data_set


def generate_text_data(data_set):
    curr_row = ""

    for row in data_set.Tables[0].Rows:
        curr_row = []
        curr_row.append(str(row[0]))
        curr_row.append(str(row[1]))
        curr_row.append(str(row[2]))

        yield "%s\r\n" % (';'.join(curr_row))


def get_table_name(node_type):
    nodetypes = {}
    nodetypes['EPG'] = 'GGSN'
    nodetypes['SBG'] = 'IMSSBG'

    if node_type in nodetypes.keys():
        node_type = nodetypes[node_type]

    sql = "SELECT top 1 typename FROM LOG_LOADSTATUS WHERE TYPENAME LIKE \
    'DC_E_"+node_type+"%' AND STATUS = 'LOADED' AND MODIFIED >= DATE(NOW()-7) ORDER \
    BY ROWCOUNT DESC, MODIFIED DESC"
    table_name=""
    try:
        conn_string = "DSN=" + data_source_name
        connection = OdbcConnection(conn_string)
        connection.Open()

        command = connection.CreateCommand()
        command.CommandText = sql
        reader = command.ExecuteReader()
        loopguard = 0

        while reader.Read() and loopguard != 1:
            table_name = reader[0]
            loopguard = 1
        connection.Close()
        if "DC_E_ERBSG2" in table_name:
            table_name = table_name.replace("DC_E_ERBSG2", "DC_E_ERBS", 1)

        Document.Properties["ConnectionError"] = ""
        Document.Properties["NoTableError"] = ""

    except TypeError:
        table_name=""
        Document.Properties["NoTableError"] = "Error: Table does not exist in ENIQ. Please install relevent Tech Pack."
    except EnvironmentError:
        Document.Properties["ConnectionError"] = "Please check ENIQ DB Connection"

    return table_name


def get_node_collection_nodetype(collection_name):
    """gets a node collection's node type with a given collection name

    Arguments:
        collection_name -- string measure

    Returns:
        selected node collection's node type
    """
    node_collection_data_table = Document.Data.Tables["NodeCollection"]
    node_collection_filter = node_collection_data_table.Select("[CollectionName] = '" + collection_name + "'")
    if not node_collection_filter.AsIndexSet().IsEmpty:
        node_type_cursor = DataValueCursor.Create(node_collection_data_table.Columns['NodeType'])
        for row in node_collection_data_table.GetRows(node_collection_filter.AsIndexSet(),node_type_cursor):
            return node_type_cursor.CurrentValue

def get_element_name(node_type):

    element_mapping_cur = create_cursor(element_mapping_table)
    element=""
    table=get_table_name(node_type)

    element_name = element_mapping_cur["ELEMENT"]
    table_name = element_mapping_cur["TABLENAME"]
    node_type_EM = elementMappingCur["Node Type"]
    row_count = element_mapping_table.RowCount

    rows_to_include = IndexSet(row_count, True)

    for row in element_mapping_table.GetRows(rows_to_include, element_name, table_name, node_type_EM):
        if table == table_name.CurrentValue.replace("_RAW",""):
            element = ((element_name.CurrentValue).split('.'))[1]

    return element


def fetch_data_from_ENIQ_async(node_type):

    try:
        if (Document.Properties["SystemArea"] != None and Document.Properties["NodeType"] != None) and (Document.Properties["SystemArea"] != "None" and Document.Properties["NodeType"] != "None"):

            try:                
                topology_fields = get_topology_table_data(node_type_original, system_area).split(',')
                topology_table = topology_fields[0]
                topology_key_field  = topology_fields[1]

                sql_topology = u"SELECT DISTINCT " + topology_key_field + "  AS node, '" + node_type_original + "' AS NodeType, '" + system_area + "' as SystemArea FROM " + topology_table + " WHERE STATUS = 'Active' ORDER BY " + topology_key_field + " ASC"
                data_set = run_query(sql_topology)
            except:
                sql_log_load = u"SELECT DISTINCT " + get_element_name(node_type) + " AS node, '" + node_type_original + "' AS NodeType, '" + system_area + "' as SystemArea FROM " + get_table_name(nodeType) + "_RAW"
                data_set = run_query(sql_log_load)

            table_name = 'NodeList'
            overall_text_data = generate_text_data(data_set)
            create_table(table_name, overall_text_data)
            Document.Properties["NoTableError"] = ""
            Document.Properties["SystemAreaErrorMsg"] = ""
        else:
            Document.Properties["SystemAreaErrorMsg"] = "Please select a System Area/Node Type"
    except:
        print "except"

def load_selected_measures():
    '''display alarm definition kpis on UI'''
    selected_measures = Document.Properties["SelectedMeasureList"].split(';')
    num_measures = 0
    for measure in selected_measures:
        num_measures += 1
        Document.Properties["SelectedKPI" + str(num_measures)] = measure

Document.Properties['ValidationError'] = ''
marked_row_selection = ""

AlarmColumn = AlarmColumn()  # Create an enum to represent column names
alarm_columns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName,
                AlarmColumn.Severity, AlarmColumn.NECollection, AlarmColumn.AlarmState, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause,
                AlarmColumn.Schedule, AlarmColumn.Aggregation, AlarmColumn.MeasureType,  AlarmColumn.SingleOrCollection,
                AlarmColumn.LookBackVal, AlarmColumn.LookBackUnit, AlarmColumn.DataRangeVal, AlarmColumn.DataRangeUnit, 
                AlarmColumn.PeriodDuration, AlarmColumn.TableTypePlaceHolder, AlarmColumn.EniqName]

alarm_definitions_data_table_name = 'Alarm Definitions'
alarm_definitions_data_table = Document.Data.Tables[alarm_definitions_data_table_name]

try:
    marked_row_selection = Document.ActiveMarkingSelectionReference.GetSelection(alarm_definitions_data_table).AsIndexSet()
except AttributeError:
    print "NoneType error" 


cursors = {column: DataValueCursor.CreateFormatted(alarm_definitions_data_table.Columns[column]) for column in alarm_columns}
src_table = Document.Data.Tables["Measure Mapping"]

filt=Document.FilteringSchemes[1][src_table][src_table.Columns["Measure"]].As[ListBoxFilter]()
filt_node_type=Document.FilteringSchemes[1][src_table][src_table.Columns["Node Type"]].As[CheckBoxFilter]()
filt_measure_type=Document.FilteringSchemes[1][src_table][src_table.Columns["Measure Type"]].As[CheckBoxFilter]()

for row in alarm_definitions_data_table.GetRows(marked_row_selection, Array[DataValueCursor](cursors.values())):
    for property_name in alarm_columns:
        if property_name != "MeasureName":
            Document.Properties[property_name.replace('_', '')] = cursors[property_name].CurrentValue
            EniqDataSource = cursors["EniqName"].CurrentValue
            Document.Properties["SelectedMeasureList"] = cursors["MeasureName"].CurrentValue
            Document.Properties["SelectedMeasureType"] = cursors["MeasureType"].CurrentValue
            

collection_type = Document.Properties[AlarmColumn.SingleOrCollection]



for r in marked_row_selection: 
   Document.Properties[AlarmColumn.NodeType] = alarm_definitions_data_table.Columns["NodeType"].RowValues.GetFormattedValue(r)
   Document.Properties[AlarmColumn.SystemArea] = alarm_definitions_data_table.Columns["SystemArea"].RowValues.GetFormattedValue(r)
   alarm_ID = alarm_definitions_data_table.Columns["AlarmID"].RowValues.GetFormattedValue(r)
   Document.Properties["CurrentAlarmID"] = alarm_ID


if Document.Properties['AlarmState'] == 'Inactive' and Document.Properties['isMarked'] == True:

    if collection_type == "Single Node":
        data_source_name = Document.Properties['ENIQDB']

        node_type = Document.Properties["NodeType"]
        node_type_original = Document.Properties["NodeType"]
        node_type = node_type.replace("-","")

        element_mapping_table = Document.Data.Tables["Measure Mapping"]
        sql = ""
        system_area = Document.Properties["SystemArea"]

        fetch_data_from_ENIQ_async(node_type)


    Document.Properties["EditBtnInput"] = "Edited"
    Document.Properties["isEdit"] = "Edit"
    Document.Properties["Action"] = "Edit"
    for page in Document.Pages:
	    if (page.Title == 'Alarm  Rules  Manager'):
		    Document.ActivePageReference=page

    load_selected_measures()

    filt_node_type.UncheckAll()
    filt_node_type.Check(Document.Properties["NodeType"])
    
    try:
        #set the node list box to selected node
        if collection_type == "Single Node":
            node_src_table = Document.Data.Tables['NodeList']
            node_filt=Document.FilteringSchemes[1][node_src_table][node_src_table.Columns["node"]].As[ListBoxFilter]()
            node_filt.IncludeAllValues=False
            node_filt.SetSelection(Document.Properties[AlarmColumn.NECollection])
        
        elif collection_type == 'Subnetwork':
            subnetworkTable = Document.Data.Tables['SubNetwork List']
            subnet_filt = Document.FilteringSchemes[2][subnetworkTable][subnetworkTable.Columns["Filtersubnetwork"]].As[ListBoxFilter]()
            subnet_filt.IncludeAllValues=False
            subnet_filt.SetSelection(Document.Properties[AlarmColumn.NECollection])
            
        else:
            collection_node_type = get_node_collection_nodetype(Document.Properties["NECollection"])
            filt_node_type.Check(collection_node_type)
        filt_measure_type.UncheckAll()
        filt_measure_type.Check(Document.Properties["SelectedMeasureType"])
    
        filt.IncludeAllValues=False
        filt.SetSelection(Document.Properties["SelectedKPI1"])

    except Exception as e:
        notify.AddWarningNotification("Exception","Error in getting selected nodes",str(e))
        print("Exception: ", e)


def run_netan_db_query(sql):
    """opens an ODBC connection and runs the SQL query passed

    Arguments:
        sql -- string of SQL query used
    Returns:
        dataset -- DataSet object created from query
    """
    try:
        connection = OdbcConnection(conn_string_netan)
        connection.Open()
        dataset = DataSet()
        adaptor = OdbcDataAdapter(sql,connection)
        adaptor.Fill(dataset)
        connection.Close()
        return dataset
    except Exception as e:
        print (e.message)


def create_data_table(dataset_text, dataset_columns, data_table_name):
    """creates a spotfire data table

    Arguments:
        dataset_text -- string text of dataset used in SQL query
        dataset_columns -- List of column names
        data_table_name -- name of table to create
    Returns:
        None
    """
    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine('|'.join(dataset_columns) + '\r\n')
    writer.Flush()
    
    for line in dataset_text:
        writer.WriteLine(line)
    
    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = "|"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(data_table_name):
        Document.Data.Tables.Remove(Document.Data.Tables[data_table_name])

    Document.Data.Tables.Add(data_table_name, fs)


def create_column_names(dataset):
    """gets a list of column names

    Arguments:
        dataset -- dataset from alarm sql
    Returns:
        List of column names
    """
    column_names = [column.ColumnName for column in dataset.Tables[0].Columns]
    return column_names  


def generate_text_data(dataset):
    """gets a list of columns that should not be removed from the data table

    Arguments:
        dataset -- dataset from alarm sql
    Returns:
        String of dataset text
    """
    for row in dataset.Tables[0].Rows:
        curr_row = []
        for column in dataset.Tables[0].Columns:
            curr_row.append(str(row[column]))

        yield "%s\r\n" % ('|'.join(curr_row))


def fetch_alarm_formulas_main():
    """main function for fetching and creating Alarm Formula table

    Arguments:
        --
    Returns:
        None
    """
    try:
        sql = """SELECT * FROM "tblAlarmFormulas" WHERE "AlarmID" = '{0}'""".format(alarm_ID)
        query_result = run_netan_db_query(sql)
        dataset_text = generate_text_data(query_result)
        dataset_columns = create_column_names(query_result)
        create_data_table(dataset_text, dataset_columns, "Alarm Formulas")
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)


Document.Properties["ExportMessage"] = ''
fetch_alarm_formulas_main()