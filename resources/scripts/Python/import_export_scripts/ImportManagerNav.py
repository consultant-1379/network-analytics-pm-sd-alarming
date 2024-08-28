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
# Name    : ImportManagerNav.py
# Date    : 3/12/2021
# Revision: 1.0
# Purpose : Naviagates to the import manager page
#
# Usage   : PM Alarm
#

from os import listdir
from os.path import isfile, join
from Spotfire.Dxp.Data import *
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService

notify = Application.GetService[NotificationService]()

def create_table(table_name, data):
    """creates a spotfire data table

    Arguments:
        data -- list of files to use for data table
        table_name -- name of table to create
    Returns:
        None
    """
    data_stream = MemoryStream()
    writer = StreamWriter(data_stream)
    header_column = table_name
    writer.WriteLine(header_column + '\r\n')
    writer.Write('\r\n'.join(data))
    writer.Flush()
    data_stream.Seek(0, SeekOrigin.Begin)
    reader_settings = TextDataReaderSettings()
    reader_settings.AddColumnNameRow(0)

    text_data_source = TextFileDataSource(data_stream, reader_settings)

    if Document.Data.Tables.Contains(table_name):
        Document.Data.Tables[table_name].ReplaceData(text_data_source)
    else:
        Document.Data.Tables.Add(table_name, text_data_source)



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


def clear_node_table(table_name):
    """ reset the node table

    Arguments:
        table_name -- name of table to clear
    Returns:
        None
    """
    data = [['node','NodeType']]
    write_data_to_table(data, table_name)


def main():
    """ main function to navigate to the page and clear out old tables

    Arguments:
        None
    Returns:
        None
    """
    path_name = "C:\\Ericsson\\PMA_Exports\\"

    # clear tables
    tables_to_clear = ["Alarm Rules - Processing", "Alarm Import Status", "Alarm Rule File Names"]
    for tbl in tables_to_clear:
        Document.Data.Tables[tbl].RemoveRows(RowSelection(IndexSet(Document.Data.Tables[tbl].RowCount, True)))

    try:
        files = [file for file in listdir(path_name) if isfile(join(path_name, file)) and 'Alarm Definitions' in file]
    except Exception as e:
        # if no folder found, just create for the user and ask them to add files
        if not os.path.exists(path_name):
            os.makedirs(path_name)
            files = []
    
    if files: 
        table_name = 'Alarm Rule File Names'
        create_table(table_name, files)
        clear_node_table('NodeList')
        Document.Properties['NECollection'] = ''
        Document.Properties['SingleNodeValue'] = ''
        Document.Properties['ImportApplyNodesError'] = ""
        Document.Properties['SingleOrCollection'] = "Collection"
        for page in Document.Pages:
            if (page.Title == 'Alarm Rules Import Manager'):                            
                Document.ActivePageReference=page
    else:
        notify.AddWarningNotification("Exception", "No files found in C:\Ericsson\PMA_Exports. Please run export process on other server.",'')

    Document.Properties['ExportMessage'] = ''
main()