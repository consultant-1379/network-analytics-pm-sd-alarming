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
# Name    : ImportRuleFileForProcessing.py
# Date    : 3/12/2021
# Revision: 1.0
# Purpose : Import a given csv file and add to table in spotfire
#
# Usage   : PM Alarm
#

from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System.Collections.Generic import List
import re
import csv


def read_csv_to_list(file):
    """reads a csv and returns as a list of list strings

    Arguments:
        file -- csv file
    Returns:
        data -- list of lists string
    """
    data = []
    with open(file, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            data.append(row)

    return data
            

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
    

def clear_import_status_table(table_name):
    """ reset the import status table to empty

    Arguments:
        table_name -- name of table to clear
    Returns:
        None
    """
    data = [['Alarm Rule','Status','Message'],['','']]
    write_data_to_table(data, table_name)


def clear_node_table(table_name):
    """ reset the import status table to empty

    Arguments:
        table_name -- name of table to clear
    Returns:
        None
    """
    data = [['node','NodeType']]
    write_data_to_table(data, table_name)


def remove_columns_from_table(columns_data):
    """ Remove the columns like alarm id etc. that wont be needed for import

    Arguments:
        columns_data -- columns to remove
    Returns:
        None
    """    

    data_table = Document.Data.Tables[columns_data['table_name']]
    delete_list = List[DataColumn]()

    for column in data_table.Columns:

        if column.Name in columns_data['cols_to_remove']:
            delete_list.Add(column)

    if delete_list.Count != 0:
        data_table.Columns.Remove(delete_list)


def main():
    """ main function to create tables and hide columns

    Arguments:
        table_name -- name of table to clear
    Returns:
        None
    """
    path_name = "C:\\Ericsson\\PMA_Exports\\"
    alarm_definition_file = Document.Properties['AlarmRuleFileName']
    file_id = re.findall(r"_(.*)", alarm_definition_file)[0]
    alarm_formula_file_name = "Alarm Formulas Export_" + file_id

    file_dict = {
        alarm_definition_file: {
            'table_name': 'Alarm Rules - Processing',
            'cols_to_remove': ['AlarmID', 'EniqID', 'NECollection', 'SingleOrCollection','EniqName']
            },
        alarm_formula_file_name: {
            'table_name': 'Alarm Formulas - Processing',
            'cols_to_remove': ['AlarmID']
        }
    }

    for file_name, details in file_dict.items():
        full_file_name = path_name + file_name
        data = read_csv_to_list(full_file_name)
        write_data_to_table(data, details['table_name'])
        remove_columns_from_table(details)

    clear_import_status_table('Alarm Import Status')
    clear_node_table('NodeList')
    
    split_file_name = alarm_definition_file.split('_')

    Document.Properties["SystemArea"] = split_file_name[1]
    Document.Properties["NodeType"] = split_file_name[2]
    Document.Properties['NECollection'] = ''
    Document.Properties['SingleNodeValue'] = ''
    Document.Properties['ImportApplyNodesError'] = ""


main()