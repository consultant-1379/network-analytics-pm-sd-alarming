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
# Name    : ImportRulesToDb.py
# Date    : 01/01/2022
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarm
#

from Spotfire.Dxp.Data.Export import DataWriterTypeIdentifiers 
from Spotfire.Dxp.Data import DataType
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Framework.ApplicationModel import  ProgressService,NotificationService
from datetime import datetime, date
import csv
import os
import clr
clr.AddReference('System.Data')
from System import Array, Byte, Threading
from System.Text import UTF8Encoding
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcType, OdbcDataAdapter
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast
import copy


ps = Application.GetService[ProgressService]()

success_dict = {}
error_dict = {}


ODBC_TRANSLATION_TABLE = {
    "integer": OdbcType.Int,
    "character varying": OdbcType.VarChar,
    "real":	OdbcType.Real,
    "date":	OdbcType.Date

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


def generate_dict_table_types(dataset):
    """gets a list of columns and data types form a dataset

    Arguments:
        dataset -- dataset from aql
    Returns:
        dict of col names and types
    """
    table_set = {}
    for row in dataset.Tables[0].Rows:
        curr_row = []
        for column in dataset.Tables[0].Columns:
            curr_row.append(str(row[column]))
        column_name = curr_row[0]
        data_type = curr_row[1]
        table_set[column_name] = ODBC_TRANSLATION_TABLE[data_type]
    return table_set


def create_value_list_for_sql(columns):
    """ create a string in the format of (?,?,?)etc. so that the correct amount of command parameters can be added.
    Arguments:
        columns -- columns used for query
    Returns:
        ?,?,? etc.
    """

    value_list = []
    for i in range(len(columns)):
        value_list.append('?')

    current_row = """{0}""".format(','.join(value_list))
 
    return current_row


def convert_type(odbctype, value):
    """ Given an odbc type, convert the value to str, int etc.
    Arguments:
        odbctype -- OdbcType
        value -- value to convert
    Returns:
        copnverted value
    """

    if odbctype == OdbcType.VarChar:
        col_val = str(value)
    elif odbctype == OdbcType.Int:
        col_val = int(value)
    elif odbctype == OdbcType.Real:
        col_val = float(value)
    elif odbctype == OdbcType.Date:
        value = value.replace(' 12:00:00 AM', '') # Depending on the server the date can be a datetime (with 12 am) added. Breaks query
        col_val = str(value)
    else:
        col_val = str(value)

    return col_val


def apply_parameters(command, parameters, column_list, type_dict):
    """ for an ODBC command, add all the required values for the parameters. varchar is default value.
    Arguments:
        command -- odbc command
        parameters -- alarm data with values etc.
        column_list -- columns to insert into db and match the names in the parmaters dict
        type_dict -- dict with type conversions of columns
    Returns:
        odbc command
    """
    parameter_index = 0
    for col,col_value in parameters.items():

        for alarm_col in column_list:
            if col_value[alarm_col] == '(Empty)':
                col_val = None
            else:
                col_val = convert_type(type_dict[alarm_col], col_value[alarm_col])

            command.Parameters.Add("@col"+str(parameter_index), type_dict[alarm_col]).Value = col_val
            parameter_index += 1

    return command


def run_netan_db_query(sql, parameters, column_list, type_dict, query_type):
    """opens an ODBC connection and runs the SQL query passed

    Arguments:
        sql -- string of SQL query used
        parameters -- dictioanry of import data/select data
        column_list -- columns for the paramter list
        type_dict -- types of values for parameters
        query_type -- insert/select
    Returns:
        dataset, message -- DataSet object created from query or can be false
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
        command = apply_parameters(command, parameters, column_list, type_dict)
        if query_type == 'select':
            dataset = DataSet()
            adaptor = OdbcDataAdapter(command)
            adaptor.Fill(dataset)
            connection.Close()
            return dataset
        else:
            command.ExecuteNonQuery()
            connection.Close()
            return True, 'Insert Data Succesful'
    
    except Exception as e:
        print e.message
        return False, 'SQL Query Error'


def create_cursor(table):
    """ Create cursors for a given table, these are used to loop through columns
    Arguments:
        table -- table name
    Returns:
        cursor
    """
    curs_list = []
    col_list = []
    for eColumn in table.Columns:
        curs_list.append(DataValueCursor.CreateFormatted(table.Columns[eColumn.Name]))
        col_list.append(table.Columns[eColumn.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict


def get_cursor_and_index(table_name, index_type, filter_col='', filter_value=''):
    """ Get a cursosr and index set for fitlering etc
    Arguments:
        table_name -- alarm def table
        index_type -- want to use a filter or no filter
        filter_col -- default val is empty in case not used
        filter_value -- default val empty if not used

    Returns:
        cursor and index set
    """
    if index_type == 'filter':  
        index_filter = Document.Data.Tables[table_name].Select("""[{filter_col}] = '{filter_value}'""".format(filter_col=filter_col, filter_value=filter_value))
        index_set = index_filter.AsIndexSet()
    else:
        index_set = IndexSet(Document.Data.Tables[table_name].RowCount, True)

    source_data_table = Document.Data.Tables[table_name]
    source_cur = create_cursor(source_data_table)

    return source_cur, index_set


def get_alarm_import_data(table_name):
    """ Get alarm dictionary of all details
    Arguments:
        table_name -- alarm def table
    Returns:
        dict with all alarm details
    """
    source_data_table = Document.Data.Tables[table_name]
    source_cur, index_set = get_cursor_and_index(table_name,'no_filter')

    data = {}

    # these columns need to be updated so they match in the database query
    replace_column_name_dict = {
        'NECollection': 'CollectionName',
        'SingleOrCollection': 'CollectionType'      
    }
          
    for value in source_data_table.GetRows(index_set, Array[DataValueCursor](source_cur.values())):
        data[source_cur['AlarmName'].CurrentValue] = {}
        for col in source_cur:
            col_name = str(col)
            if col_name in replace_column_name_dict:
                new_col_name = replace_column_name_dict[col_name]
                data[source_cur['AlarmName'].CurrentValue][new_col_name] = source_cur[col_name].CurrentValue
            else:
                data[source_cur['AlarmName'].CurrentValue][col_name] = source_cur[col_name].CurrentValue

    return data


def split_measures():  
    """ splits measures string into list. Used to check what measures in alarm
    Arguments:
        None
    Returns:
        None
    """

    current_measures = Document.Properties["SelectedMeasureList"]
    if current_measures != "":
        measures_list = current_measures.split(';')
    else:
        measures_list = []
    return measures_list


def no_match_in_columns(table, column, value):
    """ check if value found in column
    Arguments:
        table -- table to serach
        column -- column to check
        value -- value to check in column
    Returns:
        true/false value for is empty column
    """
    data_table = Document.Data.Tables[table]
    filter_val = data_table.Select("["+ column + "] = '" + value + "'")

    return filter_val.AsIndexSet().IsEmpty


def validation(alarm_name, alarm_details):
    """ validate if all fields present, kpi exists and if alarm name already used
    Arguments:
        alarm_name -- current alarm
        alarm_details -- all alarm details to be imported
    Returns:
        list of errors in validation
    """
    column_dict = ['CollectionName','CollectionType','EniqName']
    error_list = []

    # check if collection/node etc. added for each alarm
    collection_node_missing = [i for i in column_dict if i not in alarm_details]

    if collection_node_missing:
        error_log = "Collection or Single Node not added."
        error_list.append(error_log)
    else:
        # check if column for collection etc. have values added
        empty_cols = []
        for col in column_dict:
            if alarm_details[col] in ['(Empty)','']:
                empty_cols.append(col)
        if empty_cols:
            error_log = """No value assigned for column {columns}.Please add collection/single node to alarm.""".format(columns=','.join(empty_cols))
            error_list.append(error_log)

    # check if kpi available in measure mapping
    measure_mapping_data_table = Document.Data.Tables['Measure Mapping']
    current_measures_list = alarm_details['MeasureName'].split(';')
    eniq_name = alarm_details['EniqName']
    measures_not_found = []

    for measure_name in current_measures_list:
        measure_filter = measure_mapping_data_table.Select("[Measure] = '" + measure_name + "' and [DataSourceName] = '"+eniq_name+"' ")
        if measure_filter.AsIndexSet().IsEmpty:
            measures_not_found.append(measure_name)

    if measures_not_found:  
        error_log = """Measures {measures} do not exist in Measure Mapping. Cant import alarm.""".format(measures= ','.join(measures_not_found))
        error_list.append(error_log)

    # check if alarm already added to db with given name
    if not no_match_in_columns('Alarm Definitions','AlarmName',alarm_name):
        error_log = "Alarm already added to db with given name."
        error_list.append(error_log)

    # check if matching formula for alarm name
    if no_match_in_columns('Alarm Formulas - Processing','AlarmName',alarm_name):
        error_log = "Formulas not found in Alarm Formulas file for alarm."
        error_list.append(error_log)

    return ','.join(error_list)


def check_all_columns_available(db_alarm_def_columns,alarm_def_import_data, db_alarm_formula_columns):
    """ check if matching columns for all alarms in table cmopared to database table
    Arguments:
        db_alarm_def_columns -- database cols for alarm def
        alarm_def_import_data -- import columns from csv
        db_alarm_formula_columns -- column to fiter on
    Returns:
        two lists, missing cols in alarm def and alarm formulas
    """

    columns_to_ignore = ['EniqID', 'AlarmID', 'CollectionID']

    for alarm_name, details in alarm_def_import_data.items():
        import_columns = details.keys()

    missing_columns_import_alarm = [col for col in db_alarm_def_columns.keys() if col not in import_columns and col not in columns_to_ignore]

    source_data_table = Document.Data.Tables["Alarm Formulas - Processing"]
    source_cur = create_cursor(source_data_table)
    formula_import_cols = source_cur.keys()

    missing_columns_import_formula = [col for col in db_alarm_formula_columns if col not in formula_import_cols and col not in columns_to_ignore]

    return missing_columns_import_alarm, missing_columns_import_formula


def get_column_val(table, filter_value, filter_col, target_col):
    """ get a value from a table based on a filter val
    Arguments:
        table -- the table to filter
        filter_value -- value to filter
        filter_col -- column to fiter on
        target_col -- value to pick up
    Returns:
        target column value
    """
   
    source_data_table = Document.Data.Tables[table]
    source_cur, index_set = get_cursor_and_index(table,'filter',filter_col, filter_value)
       
    for value in source_data_table.GetRows(index_set, Array[DataValueCursor](source_cur.values())):
        return source_cur[target_col].CurrentValue


def get_db_columns_and_types(table):
    """ get the list of columns and datatypes. This will be used to confirm that the right columns and data types are used
        even if the db columns are changed later.
    Arguments:
        table -- the data for the alarm to be inserted
    Returns:
        dataset_text -- dataset of returned columns/types
    """
    parameters = {}
    query = """select column_name, data_type from information_schema.columns where table_name = ?"""
    column_list = ['table_name'] # the run_netan_db_query requires a column list for inserts etc. , but this is a select so so just faking the column
    type_dict = {'table_name': OdbcType.VarChar}
    parameters[0] = {'table_name' : table}
    query_result = run_netan_db_query(query, parameters, column_list, type_dict, 'select')
    dataset_text = generate_dict_table_types(query_result)

    # remove alarm id as wont be inserting it as is (will be translating using sub select etc.). These are autoincrement id in the database or foreign keys etc.
    if 'AlarmID' in dataset_text:
        del dataset_text['AlarmID']

    return dataset_text


def insert_collection_query(alarm_details, db_alarm_def_columns):
    """ insert an alarm that has a collection
    Arguments:
        alarm_details -- the data for the alarm to be inserted
        db_alarm_def_columns -- required columns to match and insert to db
    Returns:
        result of query (True/False) and the message from the sql query
    """
    parameters = {}

    # add eniq id info and collection id
    alarm_details['EniqID'] = int(get_column_val('EniqEnmMapping', alarm_details['EniqName'], 'EniqName','EniqID'))
    alarm_details['CollectionID'] =  int(get_column_val('NodeCollection', alarm_details['CollectionName'], 'CollectionName','CollectionID'))

    table = 'tblAlarmDefinitions'
    column_list =[col for col in db_alarm_def_columns.keys()]
    column_values = create_value_list_for_sql(column_list)
    columns = ','.join(["\"" + col + "\"" for col in column_list])

    query = """INSERT INTO "{table}" ({columns}) VALUES ({values})""".format(table=table,columns=columns,values=column_values)
    parameters[0] = alarm_details

    query_result, query_message = run_netan_db_query(query, parameters, column_list, db_alarm_def_columns, 'insert')

    return query_result, query_message


def insert_formulas_query(formula_columns, db_alarm_formula_columns):
    """ insert the matching alarm formulas to the alarm formula table
    Arguments:
        formula_columns -- the data for the formulas to be inserted
        db_alarm_formula_columns -- required columns to match and insert to db
    Returns:
        result of query (True/False) and the message from the sql query
    """

    modified_db_alarm_formula_columns = copy.deepcopy(db_alarm_formula_columns)
    column_list =[col for col in modified_db_alarm_formula_columns.keys()]
    table = 'tblAlarmFormulas'
    inserts = []

    for i in formula_columns:
        row_values = create_value_list_for_sql(column_list)
        #modify the value list to add 'select id for alarm id'
        row_values += """,(select "AlarmID" from "tblAlarmDefinitions" where "AlarmName" = ?)"""
        inserts.append('({})'.format(row_values))

    column_values = ','.join(inserts)
    column_list.append('AlarmID')
    columns = ','.join(["\"" + col + "\"" for col in column_list])

    modified_db_alarm_formula_columns['AlarmID'] = OdbcType.VarChar

    query = """INSERT INTO "{table}" ({columns}) VALUES {values}""".format(table=table,columns=columns,values=column_values)

    query_result, query_message = run_netan_db_query(query, formula_columns, column_list, modified_db_alarm_formula_columns, 'insert')

    return  query_result, query_message


def insert_single_node_or_subnetwork_collection(alarm_collection_details, db_collection_columns):
    """ geenrate the query for inseting the single node to the collection
    Arguments:
        alarm_collection_details -- details of node type, system area etc.
        db_collection_columns -- required columns to match and insert to db
    Returns:
        colleciton query -- query for inserting single node
    """
  
    # remove these only for this part, these are needed elsewhere so make a deepcopy and remove
    modified_db_col = copy.deepcopy(db_collection_columns)
    alarm_collection_details['EniqID'] = int(get_column_val('EniqEnmMapping', alarm_collection_details['EniqName'], 'EniqName','EniqID'))
    if alarm_collection_details['CollectionType'] == 'Single Node':
		remove_col_list = ['CollectionID', 'EniqID','TypeOfCollection']
    else:
		remove_col_list = ['CollectionID','TypeOfCollection']

    for i in remove_col_list:
        del modified_db_col[i]

    alarm_collection_details["CreatedBy"] = Threading.Thread.CurrentPrincipal.Identity.Name
    alarm_collection_details["CreatedOn"] = str(date.today())
    #alarm_collection_details["TypeOfCollection"] = NULL

    column_list =[col for col in modified_db_col.keys()]
    column_values = ','.join(["""'{val}'""".format(val =alarm_collection_details[col]) for col in column_list])
    columns = ','.join(["\"" + col + "\"" for col in column_list])

    collection_query = """INSERT INTO "tblCollection" ({columns}) VALUES ({values})""".format(columns=columns,values=column_values)

    return collection_query


def insert_single_node_or_subnetwork_query(alarm_details, db_alarm_def_columns, db_collection_columns):
    """ insert a single node alarm. Add single to the tblcollection table. Needed as part of single node alarm
        Two requests ran in same session so currval can be used to add colleciton id to alarm
    Arguments:
        alarm_details -- dict for all alarm info being imported
        db_alarm_def_columns -- datbase alarm columns used to ensure that import matches columns exactly
        db_collection_columns -- datbase collection columns used to ensure that import matches columns exactly
    Returns:
        result of query (True/False) and the message from the sql query
    """

    # isnert single node into collection table
    cq = insert_single_node_or_subnetwork_collection(alarm_details, db_collection_columns)
    alarm_details['EniqID'] = int(get_column_val('EniqEnmMapping', alarm_details['EniqName'], 'EniqName','EniqID'))
    modified_db_alarm_def_cols = copy.deepcopy(db_alarm_def_columns)

    # insert alarm data with collection id = single node just added
    column_list =[col for col in modified_db_alarm_def_cols.keys()]
    table = 'tblAlarmDefinitions'
      
    column_list =[col for col in modified_db_alarm_def_cols.keys() if col != 'CollectionID']
     
    column_values = create_value_list_for_sql(column_list)
    column_values += """,currval('"tblCollection_seq"')"""
    column_list.append('CollectionID')

    columns = ','.join(["\"" + col + "\"" for col in column_list])
    del modified_db_alarm_def_cols['CollectionID']
    column_list.remove('CollectionID')

    query = """INSERT INTO "{table}" ({columns}) VALUES ({values})""".format(table=table,columns=columns,values=column_values)
    full_query = cq + '; ' + query

    parameters = {}
    parameters[0] = alarm_details

    query_result, query_message = run_netan_db_query(full_query, parameters, column_list, modified_db_alarm_def_cols, 'insert')

    return  query_result, query_message


def get_calculated_columns(table, alarm_name, db_alarm_formula_columns):
    """ get the list of columns used in the alarm formulas table
    Arguments:
        data -- list of files to use for data table
        table_name -- name of table to create
    Returns:
        list of formulas
    """
    source_data_table = Document.Data.Tables[table]
    source_cur, index_set = get_cursor_and_index(table,'filter', 'AlarmName',alarm_name)

    parameter_index = 0 
    paramater_list = {}

    for value in source_data_table.GetRows(index_set, Array[DataValueCursor](source_cur.values())):
        for column_name in db_alarm_formula_columns.keys():

            if parameter_index in paramater_list:
                paramater_list[parameter_index][column_name] = source_cur[column_name].CurrentValue 
            else:
                paramater_list[parameter_index] = { 
                    column_name:source_cur[column_name].CurrentValue          
                }
        
        # there are some queries used where the alarm id will need to be inserted as a
        # sub query. This alarm name is used as part of a subquery and stored in alarmid col
        paramater_list[parameter_index]['AlarmID'] = paramater_list[parameter_index]['AlarmName'] 
        parameter_index += 1

    return paramater_list


def write_data_to_table(data,table_name):
    """creates a spotfire data table
    Arguments:
        data -- list of files to use for data table
        table_name -- name of table to create
    Returns:
        None
    """
    data_stream = MemoryStream()
    writer = StreamWriter(data_stream)
    for line in data:
        writer.WriteLine('|'.join(line))
    writer.Flush()
    data_stream.Seek(0, SeekOrigin.Begin)
    reader_settings = TextDataReaderSettings()
    reader_settings.Separator = "|"
    reader_settings.AddColumnNameRow(0)

    text_data_source = TextFileDataSource(data_stream, reader_settings)

    if Document.Data.Tables.Contains(table_name):
        Document.Data.Tables[table_name].ReplaceData(text_data_source)
    else:
        Document.Data.Tables.Add(table_name, text_data_source)
    

def create_result_table(table_name, error_dict, success_dict):
    """ create summary table for import

    Arguments:
        table_name -- name of table to clear
        error_dict -- dict of errors from import
        success_dict -- dict of success from import
    Returns:
        None
    """

    data = [['Alarm Rule','Status','Message']]
    dict_list = [error_dict, success_dict]

    for dict in dict_list:
        for alarm_name, message in dict.items():
            split_msg = message.split('|')
            data.append([alarm_name,split_msg[0],split_msg[1]])

    write_data_to_table(data, table_name)


def process_and_import_alarms():
    """ checks for validation errors and imports the alarms

    Arguments:
        None
    Returns:
        None
    """

    # get alarm to be imported information
    alarm_def_import_data = get_alarm_import_data('Alarm Rules - Processing')

    if alarm_def_import_data:
    
        db_alarm_def_columns = get_db_columns_and_types('tblAlarmDefinitions')
        db_alarm_formula_columns = get_db_columns_and_types('tblAlarmFormulas')
        db_collection_columns = get_db_columns_and_types('tblCollection')
        
        # check first if all the columns in the import tables match the destination table in the database
        missing_columns_import_alarm, missing_columns_import_formula = check_all_columns_available(db_alarm_def_columns, alarm_def_import_data, db_alarm_formula_columns)
    
        curr_count = 0
        formula_status_message = ''
        total_alarms = len(alarm_def_import_data.keys())

        if not missing_columns_import_alarm and not missing_columns_import_formula:
            for alarm_name, alarm_details in alarm_def_import_data.items():

                curr_count += 1
                ps.CurrentProgress.ExecuteSubtask("""Processing Alarm Rule {curr} of {total}""".format(curr=curr_count, total = total_alarms))
                ps.CurrentProgress.ExecuteSubtask('Processing Alarm Rule {alarm_name}'.format(alarm_name=alarm_name))
                errors = []
                query_result_formula = ''
                query_result_alarm = ''

                #for each alarm, validate that the required data is present
                errors = validation(alarm_name, alarm_details)

                if not errors:
                    formula_columns = get_calculated_columns('Alarm Formulas - Processing', alarm_name, db_alarm_formula_columns)
                    
                    if alarm_details['CollectionType'] == 'Collection':
                        query_result_alarm, alarm_status_message = insert_collection_query(alarm_details, db_alarm_def_columns)                   
                    else:           
                        query_result_alarm, alarm_status_message = insert_single_node_or_subnetwork_query(alarm_details, db_alarm_def_columns, db_collection_columns)
                    
                    if query_result_alarm:
                        query_result_formula, formula_status_message = insert_formulas_query(formula_columns, db_alarm_formula_columns) 

                    if query_result_alarm and query_result_formula:
                        success_dict[alarm_name] = 'Import Successful| '
                    else:
                        error_dict[alarm_name] = 'Import Failed|SQL query Result - Alarm Definitions: ' + str(alarm_status_message) + ', SQL query Result - Alarm Formulas: '+ str(formula_status_message)
                else:
                    error_dict[alarm_name] = 'Import Failed|' + errors
    
        else:
            error_dict['All Alarms'] = 'Import Failed|Required columns missing from {missing_alarm_cols} {missing_formula_cols}'.format(missing_alarm_cols=missing_columns_import_alarm, missing_formula_cols=missing_columns_import_formula)
    else:
        Document.Properties['ImportApplyNodesError'] = "No file selected. Please select a file to import."

def main():
    """ main function to trigger ps process
    Arguments:
        None
    Returns:
        None
    """
    Document.Properties['ImportApplyNodesError'] = ''
    #ps.ExecuteWithProgress('Importing Alarms', 'In Progress...', process_and_import_alarms)
    process_and_import_alarms()

    if error_dict or success_dict:
        create_result_table('Alarm Import Status',error_dict, success_dict)

    #refresh alarm definiton table
    Document.Data.Tables['Alarm Definitions'].Refresh()


main()