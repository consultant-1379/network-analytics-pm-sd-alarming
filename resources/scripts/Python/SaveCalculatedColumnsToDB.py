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
# Name    : SaveCalculatedColumnsToDB.py
# Date    : 19/11/2021
# Revision: 3.0
# Purpose : 
#
# Usage   : PM Alarms
#
from System import Environment
from datetime import date
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
import clr
clr.AddReference('System.Data')
from System import Environment, Threading, Array
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System.Data.Odbc import OdbcConnection, OdbcCommand, OdbcType
from Spotfire.Dxp.Data import *
from System.Collections.Generic import Dictionary, List
from System import DateTime
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast
from collections import OrderedDict
import re


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

try:
    conn_string= Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)

username = Threading.Thread.CurrentPrincipal.Identity.Name


def create_cursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""

    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))

    return cusrDict


def run_netan_sql_param(sql,query_parameters, column_list):
    """ Run a SQL query using ODBC connection """

    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, query_parameters, column_list)
        command.ExecuteNonQuery()

        connection.Close()
    except Exception as e:
        print(e.message)
        raise


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


def apply_parameters(command, query_parameters, column_list):
    """ for an ODBC command, add all the required values for the parameters."""

    parameter_index = 0

    for col,col_value in query_parameters.items():
        # need to be added in correct order, so use the column_list to define the order
        for column_name, odbc_col_type in column_list.items(): 
            command.Parameters.Add("@col"+str(parameter_index), odbc_col_type).Value = str(col_value[column_name])
            parameter_index += 1

    return command


def get_calculated_columns(table_name):
    """gets a dictionary of calulated columns and their expressions

    Arguments:
        table_name -- Name of data table to retrieve calculated columns from
    Returns:
        calc_columns_dict -- String dict of calulated columns and their expressions
        calc_types_dict -- String dict of calulated columns and their data types
    """

    calc_columns_dict = {}
    calc_types_dict = {}
    data_table = Document.Data.Tables[table_name]
    column_collection = data_table.Columns

    for column in column_collection:
        if column.Properties.ColumnType == DataColumnType.Calculated:
            calc_col = column.Properties.GetProperty("Expression")

            if column.Properties.DataType.ToString() == "String":
                calc_col = calc_col.replace("\"","'")

            calc_columns_dict[column.Name] = calc_col.replace('\r\n', ' ')
            calc_types_dict[column.Name] = column.Properties.DataType.ToString()

    return calc_columns_dict, calc_types_dict


def insert_alarm_formulas(alarm_name):
    """ inserts formulas in to tblAlarmFormulas

    Arguments:
        alarm_name -- Name of alarm used in SQL query
    Returns:
        none
    """
    paramater_list = {}
    alarm_ID = get_alarm_id(alarm_name)

    columns_for_insert_dict = OrderedDict(
        [
            ("AlarmName",OdbcType.VarChar), 
            ("AlarmColumnName", OdbcType.VarChar), 
            ("AlarmColumnFormula", OdbcType.VarChar), 
            ("AlarmID", OdbcType.Int), 
            ("AlarmColumnDataType", OdbcType.VarChar)
        ]
    )

    columns_for_insert = ["""\"{0}\"""".format(column) for column in columns_for_insert_dict]

    sql_query = """INSERT INTO "tblAlarmFormulas" ({0}) VALUES """.format(','.join(columns_for_insert))

    calc_columns_dict, calc_types_dict = get_calculated_columns(alarm_name)

    parameter_index = 0
    for column_name, column_expression in calc_columns_dict.items():
        paramater_list[parameter_index] = {
            "AlarmName": alarm_name, 
            "AlarmColumnName": column_name, 
            "AlarmColumnFormula": column_expression,
            "AlarmID": alarm_ID, 
            "AlarmColumnDataType": calc_types_dict[column_name]
        }
        parameter_index += 1

    sql_query += create_value_list_for_sql(paramater_list, columns_for_insert)

    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)


def delete_from_alarm_formula_table(alarm_name):
    """Removes any invalid columns

    Arguments:
        alarm_name -- Name of alarm used in SQL 
    Returns:
        none
    """
    paramater_list = {}
    sql_query = """DELETE FROM "tblAlarmFormulas" WHERE "AlarmName" = ?"""
    columns_for_insert_dict = {"AlarmName":OdbcType.VarChar}
    paramater_list['delete_query'] = {"AlarmName":alarm_name}

    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)


def update_alarm_query(alarm_name):
    ''' write the alarm query to the database '''
    alarm_query = Document.Properties["CurrentSQLQuery"]
    paramater_list = {}

    sql_query = '''
                UPDATE "tblAlarmDefinitions"
                SET "AlarmQuery" = ?
                WHERE "AlarmName" = ?;
                '''
    columns_for_insert_dict = OrderedDict(
        [
            ("AlarmQuery", OdbcType.VarChar),
            ("AlarmName",OdbcType.VarChar)
        ]
    )
    paramater_list['update_query'] = {"AlarmQuery":alarm_query,"AlarmName":alarm_name}
    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)


def get_alarm_name_from_table():
    """gets alarm name from current page

    Arguments:
        None
    Returns:
        alarm_name -- Name of alarm to be used in SQL query
    """
    alarm_name = ""
    for vis in Application.Document.ActivePageReference.Visuals:
        if vis.Title != "Save Navigation":
            alarm_name = vis.Title

    return alarm_name


def remove_page(alarm_name):
    """Removes the newly created save page

    Arguments:
        alarm_name -- Name of alarm used in SQL query
    Returns:
        None
    """
    page_name = alarm_name
    for page in Document.Pages:
        if page.Title == page_name:
            Document.Pages.Remove(page)


def remove_table(alarm_name):
    """Removes the newly created table from fetch data

    Arguments:
        alarm_name -- Name of alarm used in SQL query
    Returns:
        None
    """
    table_name = alarm_name
    data_table = Document.Data.Tables[table_name]
    Document.Data.Tables.Remove(data_table)


def remove_invalid_columns(table_name):
    """Removes any invalid columns

    Arguments:
        table_name -- Name of table to remove columns from 
    Returns:
        None
    """
    data_table = Document.Data.Tables[table_name]
    column_collection = data_table.Columns

    for column in column_collection:
        if column.Name in get_invalid_cols(table_name):
            column_collection.Remove(column)


def navigate_to_page():
    """Navigates back to Alarm Rules Manager page

    Arguments:
        None
    Returns:
        None
    """
    for page in Document.Pages:
        if page.Title == "Alarm Rules Manager":
            Document.ActivePageReference=page


def get_alarm_id(alarm_name):
    """Gets the current Alarm_ID

    Arguments:
        None
    Returns:
        Alarm_ID
    """
    alarm_definitions_table_name = 'Alarm Definitions'
    alarm_definitions_data_table = Document.Data.Tables[alarm_definitions_table_name]
    alarm_definitions_table_cursor = create_cursor(alarm_definitions_data_table)
    alarm_ID = 0
    table_filter = alarm_definitions_data_table.Select("[AlarmName]= '" + alarm_name + "'")

    for alarm in alarm_definitions_data_table.GetRows(table_filter.AsIndexSet(), Array[DataValueCursor](alarm_definitions_table_cursor.values())):
        if alarm_name == alarm_definitions_table_cursor['AlarmName'].CurrentValue:
            alarm_ID = alarm_definitions_table_cursor['AlarmID'].CurrentValue

    return alarm_ID


def get_invalid_cols(data_table_name):
    """Gets a list of invalid columns to be removed

    Arguments:
        data_table_name - String name of table to remove columns from 
    Returns:
        invalid_cols - String list of invalid columns
    """
    data_table = Document.Data.Tables[data_table_name]
    column_collection = data_table.Columns
    invalid_cols = []
    for column in column_collection:
        if column.Properties.GetProperty("ColumnType") == column.Properties.GetProperty("ColumnType").Calculated:
            calc_col = Document.Data.Tables[data_table_name].Columns[column.Name].As[CalculatedColumn]()
            is_valid = column.Properties.GetProperty("isValid")

            if "Invalid column" in calc_col.Expression or not is_valid:
                invalid_cols.append(column.Name)

    return invalid_cols
                

def check_for_alarm_criteria_col(data_table_name):
    """Checks if there is a column in the table called ALARM_CRITERIA columns to be remove"""

    data_table = Document.Data.Tables[data_table_name]
    column_collection = data_table.Columns
    for column in column_collection:
        if column.Name == "ALARM_CRITERIA":
            return True

    return False


def is_empty_expression(expr):
    """checks if the expression of objectOfReference column an empty string"""

    string = str(expr).replace(" ", "")
    string = re.sub(r'[\\n\\r\\t]', '', string)

    return string == '""'
        

def check_object_of_reference_col(data_table_name):
    """Checks if there is a column in the table called ObjectOfReference columns to be remove or be set to invalid string"""

    data_table = Document.Data.Tables[data_table_name]
    column_collection = data_table.Columns
    for column in column_collection:
        if column.Name == "ObjectOfReference":
            expression = column.Properties.GetProperty("Expression")
            return not is_empty_expression(expression)

    return False



def main():
    """ main function """

    alarm_name = get_alarm_name_from_table()
    
    invalid_cols_error_msg = "The columns {invalid_cols} have invalid column expressions. To proceed either resolve these expressions manually or remove the columns."
    alarm_criteria_error = "The column named ALARM_CRITERIA must exist in the table to proceed"
    object_of_reference_error = "The column named ObjectOfReference must exist and with valid value in the table to proceed"

    invalid_cols = get_invalid_cols(alarm_name)

    if len(invalid_cols) > 0:
        Document.Properties["SaveColumnsErrorMsg"] = invalid_cols_error_msg.format(invalid_cols = ', '.join(invalid_cols))
    elif not check_for_alarm_criteria_col(alarm_name):
        Document.Properties["SaveColumnsErrorMsg"] = alarm_criteria_error
    elif not check_object_of_reference_col(alarm_name):
        Document.Properties["SaveColumnsErrorMsg"] = object_of_reference_error
    else:
        if Document.Properties["IsEdit"] == "Edit":
            try:
                delete_from_alarm_formula_table(alarm_name)
                remove_table("Alarm Formulas")
            except:
                print(e.message)
                Document.Properties["SaveColumnsErrorMsg"] = "Error updating alarm formulas."
            
        try:
            insert_alarm_formulas(alarm_name)
        except Exception as e:
            print(e.message)
            Document.Properties["SaveColumnsErrorMsg"] = "Error saving calculated columns."

        try:
            update_alarm_query(alarm_name)
        except Exception as e:
            print(e.message)
            Document.Properties["SaveColumnsErrorMsg"] = "Error updating alarm formulas."

        navigate_to_page()

        remove_page(alarm_name)
        remove_table(alarm_name)

        Document.Properties["SaveColumnsErrorMsg"] = ""


main()
