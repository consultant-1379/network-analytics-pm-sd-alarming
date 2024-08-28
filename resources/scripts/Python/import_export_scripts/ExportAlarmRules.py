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
# Name    : ExportAlarmRules.py
# Date    : 04/11/2021
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarm
#

from Spotfire.Dxp.Data.Export import DataWriterTypeIdentifiers 
from Spotfire.Dxp.Data import DataType
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from datetime import datetime
import csv
import os
import clr
clr.AddReference('System.Data')
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcType, OdbcDataAdapter
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast


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

_key = ast.literal_eval(Document.Properties['valArray'])
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
        notify.AddWarningNotification("Exception", "Error in DataBase Connection", str(e))
        print("Exception: ", e)
        raise


def create_data_table(dataset_text, dataset_columns, column_types, data_table_name):
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

    for i in range(len(column_types)):
        settings.SetDataType(i, column_types[i])

    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(data_table_name):
        Document.Data.Tables[data_table_name].ReplaceData(fs)
    else:
        Document.Data.Tables.Add(data_table_name, fs)


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


def create_value_list_for_sql(alarm_ids):  
    """create a string in the format of (?,?,?)etc. so that the correct amount of command parameters can be added.

    Arguments:
        alarm_ids -- id list. Used to count how many parmaters needed for string
    Returns:
        parameter_list -- a string in the format of (?,? etc.)
    """

    value_list = ['?' for i in range(len(alarm_ids))]
    parameter_list = """({0})""".format(','.join(value_list))

    return parameter_list


def run_netan_db_query(sql, alarm_ids):
    """opens an ODBC connection and runs the SQL query passed

    Arguments:
        sql -- string of SQL query used
    Returns:
        dataset -- DataSet object created from query
    """
    try:
        try:
            conn_string_netan = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
        except Exception as e:
            notify.AddWarningNotification("Exception","Error in DataBase Connection", str(e))
            print("Exception: ", e)

        connection = OdbcConnection(conn_string_netan)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql

        for i in alarm_ids:
            command.Parameters.Add("@alarm_ids" + i, OdbcType.Int).Value = int(i)

        dataset = DataSet()
        adaptor = OdbcDataAdapter(command)
        adaptor.Fill(dataset)
        connection.Close()

        return dataset
    except Exception as e:
        print("Exception: ", e)
        raise


def export_alarm_rules(table_name, file_id, alarms):
    """for each sys_area/node_type, filter the alarm rules and alarm formulas tables and export as csv

    Arguments:
        table_name -- string of SQL query used
        file_id -- file_id in the form of a date string
        alarms -- alarm dict with alarm sys area/node type and ids
    Returns:
        --
    """
    path_name = "C:\\Ericsson\\PMA_Exports\\"

    table = Document.Data.Tables[table_name]
    headers = []

    for col in table.Columns:
        headers.append(col.Name)

    for alarm_type, alarm_ids in alarms.items():

        filtercondition = """[AlarmID] IN ({ids}) """.format(ids=','.join(alarm_ids['alarm_id']))
        index_filter = table.Select(filtercondition)
        selection = index_filter.AsIndexSet()

        file_name = """{name}_{alarm_type}_{file_id}.csv""".format(name=table_name, alarm_type=alarm_type, file_id=file_id)

        if not os.path.exists(path_name):
            os.makedirs(path_name)

        rows = []
        for r in selection:
            curr_row = []
            for column in headers:
                col_value = table.Columns[column].RowValues.GetFormattedValue(r)
                if col_value == "(Empty)":
                    col_value = None
                curr_row.append(col_value)
            rows.append(curr_row)

        with open("""{path_name}{file_name}""".format(path_name=path_name, file_name=file_name), 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)   

    return os.path.exists(path_name+file_name)


def get_column_names_and_types(data_set):
    """for a dataset object, return the column types(refering to a type dict) and names

    Arguments:
        data_set -- ODBC Dataset
    Returns:
        column_names, column_types -- column names and types as lists
    """
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


def get_data_from_query(sql_partial, alarm_ids, table_name):
    """ fetches and creates Alarm Formula table for all alarm ids

    Arguments:
        alarms -- a dict for alarm tpyes and ids
    Returns:
        None
    """
    try:
        parameterized_alarm_ids = create_value_list_for_sql(alarm_ids)
        sql = """{sql_partial}{parameters}""".format(sql_partial=sql_partial, parameters=parameterized_alarm_ids)
        query_result = run_netan_db_query(sql, alarm_ids)
        dataset_text = generate_text_data(query_result)
        dataset_columns, dataset_column_types = get_column_names_and_types(query_result)

        create_data_table(dataset_text, dataset_columns, dataset_column_types, table_name)
    except Exception as e:
        notify.AddWarningNotification("Exception", "Error in DataBase Connection", str(e))
        print("Exception: ", e)
        raise


def get_selected_alarms():
    """creates a dict of selected alarm rules split by sysarea/nodetype

    Arguments:
        --
    Returns:
        alarms -- dictionary {'Radio_ERBS':{'alarm_ids':['1','2']}}
    """
    alarms = {}

    alarm_definitions_data_table_name = 'Alarm Definitions'
    table = Document.Data.Tables[alarm_definitions_data_table_name]

    selection = Document.ActiveFilteringSelectionReference.GetSelection(table).AsIndexSet()
    selection = Document.Data.Markings["Marking"].GetSelection(table).AsIndexSet()

    for r in selection:
        alarm_id = table.Columns["AlarmID"].RowValues.GetFormattedValue(r)
        system_area = table.Columns["SystemArea"].RowValues.GetFormattedValue(r)
        node_type = table.Columns["NodeType"].RowValues.GetFormattedValue(r)

        system_node = """{system_area}_{node_type}""".format(system_area=system_area, node_type=node_type)

        if system_node not in alarms:
            alarms[system_node] = {'alarm_id': list()}
        alarms[system_node]['alarm_id'].append(alarm_id)

    return alarms


def main():
    """main function to export alarm rules and formulas to a csv file

    Arguments:
        --
    Returns:
        --
    """
    alarms = get_selected_alarms()
    
    data_to_export = {
        'alarm_def_details': {
            'table_name': 'Alarm Definitions Export',
            'sql': """SELECT * FROM "vwAlarmDefinitions" WHERE "AlarmID" IN"""
            },
        'alarm_formula_details': {
            'table_name': 'Alarm Formulas Export',
            'sql': """SELECT * FROM "tblAlarmFormulas" WHERE "AlarmID" IN"""
        }
    }

    if alarms:
        alarm_ids = [alarm_id for alarm_list in alarms.values() for alarm_id in alarm_list['alarm_id']]
        file_id = str(datetime.today().strftime('%Y%m%d_%H%M%S'))
        result_list = []
        try:
            for data in data_to_export.values():
                get_data_from_query(data['sql'], alarm_ids, data['table_name'])
                result_list.append(export_alarm_rules(data['table_name'], file_id, alarms))
        except Exception as e:
            notify.AddWarningNotification("Exception", "Error exporting alarms.", str(e))
            Document.Properties['ExportMessage'] = "No alarm rules exported."

        # If all alarms exported then give message
        if False not in result_list:
            Document.Properties['ExportMessage'] = "Rules exported to C:\\Ericsson\\PMA_Exports"
            

main()
