# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2022 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : ApplyNodesCollectionImport.py
# Date    : 1/1/2022
# Revision: 1.0
# Purpose : Apply the selected nodes/collection to selected rows for import
#
# Usage   : PM Alarm
#

from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System import Array
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Application.Visuals import TablePlot


def create_cursor(data_table):
    """Create cursors for a given table, these are used to loop through columns"""
    curs_list = []
    col_list = []
    for curr_col in data_table.Columns:
        curs_list.append(DataValueCursor.CreateFormatted(data_table.Columns[curr_col.Name]))
        col_list.append(data_table.Columns[curr_col.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))
    return cusr_dict


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


def generate_data(table, list_of_alarms_to_edit, column_list_cursor, collection_type, node_column_value):
    """ generate data dictionary to create a table with
    Arguments:
        table -- table to read from
        list_of_alarms_to_edit -- alarms that have been selected to edit
        column_list_cursor -- column names etc.
        collection_type -- depending on type, assign to the column dict
        node_column_value -- collection or single node
    Returns:
        data_rows -- data used to create table
    """
    eniq_db = Document.Properties['ENIQDataSourcesDropDown']

    column_dict = {
        'NECollection': node_column_value,
        'SingleOrCollection': collection_type,
        'EniqName': eniq_db
    }

    column_list = [col.Name for col in table.Columns]

    for col in column_dict.keys():
        if col not in column_list:
            column_list.append(col)   
    
    data_rows = []
    data_rows.append(column_list)
        
    for row in table.GetRows(Array[DataValueCursor](column_list_cursor.values())):
        curr_alarm_name = column_list_cursor['AlarmName'].CurrentValue
        row_string  = []

        for column in column_list:
            if column in column_dict and curr_alarm_name in list_of_alarms_to_edit:
                write_value = column_dict[column]
            elif column not in column_list_cursor.keys():
                write_value = ''
            else:
                write_value = column_list_cursor[column].CurrentValue                
            row_string.append(write_value)
        data_rows.append(row_string)

    return data_rows


def show_added_columns():
    """ when columns are added to data table, need to show them.
        Uses tablePlot variable that is passed to the script in spotfire
    Arguments:
        --
    Returns:
        --
    """
    # Get a handle to the data table
    # tablePlot is the visualization parameter passed to the script to specify the table to work on.
    dataTable = tablePlot.As[TablePlot]().Data.DataTableReference

    # Get a handle to the table plot
    table = tablePlot.As[TablePlot]()

    # Get the ColumnCollection for the table plot
    columns = table.TableColumns
    column_check =  [i.Name for i in columns]

    col_list = ['NECollection', 'SingleOrCollection', 'EniqName']

    for i in col_list:
        if i not in column_check:
            columns.Add(dataTable.Columns.Item[i])


def main():
    """ main function to apply nodes/collection to selected alarm rules
    Arguments:
        --
    Returns:
        --
    """
    Document.Properties['ImportApplyNodesError'] =  ''
    alarm_rules_processing_table = Document.Data.Tables["Alarm Rules - Processing"]
    destination_table = "Alarm Rules - Processing"
    list_of_alarms_to_edit = []
    
    markings = Document.Data.Markings['Marking'].GetSelection(alarm_rules_processing_table)
    column_list_cursor = create_cursor(alarm_rules_processing_table)

    collection_type = Document.Properties['SingleOrCollection']

    if collection_type == 'Collection':
        node_column_value = Document.Properties['NECollection']
    elif collection_type == 'Subnetwork':
        node_column_value = Document.Properties['subnetwork']
    else:
        node_column_value = Document.Properties['SingleNodeValue']

    if markings.IncludedRowCount != 0 and node_column_value != '':
        for row in alarm_rules_processing_table.GetRows(markings.AsIndexSet(), Array[DataValueCursor](column_list_cursor.values())):
            list_of_alarms_to_edit.append(column_list_cursor['AlarmName'].CurrentValue)

        data = generate_data(alarm_rules_processing_table, list_of_alarms_to_edit, column_list_cursor, collection_type, node_column_value)

        write_data_to_table(data, destination_table)
        show_added_columns()

    else:
        Document.Properties['ImportApplyNodesError'] = "An alarm rule must be selected to assign nodes or a collection."

main()