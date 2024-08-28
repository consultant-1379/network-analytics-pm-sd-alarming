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
# Name    : ClearAlarmDefinitionsValues.py
# Date    : 04/01/2021
# Revision: 2.0
# Purpose : Reset Alarm Rules Editor to default values once new Alarm
#           Defiition is created or changed and when switching between
#           tabs.
#
# Usage   : PM Alarming
#

from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Application.Filters import ListBoxFilter

def create_table(data_table_name, stream):
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

def change_view():

    table_name = 'NodeList'
    stream = MemoryStream()
    csv_writer = StreamWriter(stream)
    csv_writer.WriteLine("node;NodeType\r\n")
    csv_writer.Flush()
    create_table(table_name, stream)

    for page in Document.Pages:
        if page.Title == 'Alarm Rules Manager':
            Document.ActivePageReference = page

def reset_values():
    "Resets UI inputs and filters to default values"
    Document.Properties['AlarmName'] = ''
    Document.Properties['AlarmType'] = 'Threshold'
    Document.Properties['NECollection'] = ''
    Document.Properties['KPIType'] = 'Counter'
    Document.Properties['Condition'] = '<='
    Document.Properties['Severity'] = 'MINOR'
    Document.Properties['Schedule'] = '15'
    Document.Properties['ProbableCause'] = ''
    Document.Properties['SpecificProblem'] = ''
    Document.Properties['SelectedKPI1'] = ''
    Document.Properties['SelectedKPI2'] = ''
    Document.Properties['SelectedKPI3'] = ''
    Document.Properties['SelectedKPI4'] = ''
    Document.Properties['SelectedMeasureList'] = ''
    Document.Properties["ErrorLabelMultipleNodeSelected"]=''
    Document.Properties["ErrorLabelMultipleMeasureSelected"]=''
    Document.Properties["Aggregation"] = 'None'
    Document.Properties['LookbackVal'] = '1'
    Document.Properties['LookbackUnit'] = 'ROP'
    Document.Properties['DataRangeVal'] = '1'
    Document.Properties['DataRangeUnit'] = 'ROP'

change_view()
reset_values()