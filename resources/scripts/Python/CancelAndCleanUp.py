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
# Name    : CancelAndCleanUp.py
# Date    : 26/11/2021
# Revision: 2.0
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
from Spotfire.Dxp.Application.Filters import ListBoxFilter
from System.Data.Odbc import OdbcConnection, OdbcCommand, OdbcType
from Spotfire.Dxp.Data import *
from System.Collections.Generic import Dictionary, List
from System import DateTime


def create_cursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""

    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))

    return cusrDict

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


def reset_values():
    """Resets UI inputs and filters to default values"""
    Document.Properties['AlarmName'] = ''
    Document.Properties['AlarmType'] = 'Threshold'
    Document.Properties['NECollection'] = ''
    Document.Properties['KPIType'] = 'Counter'
    Document.Properties['Severity'] = 'MINOR'
    Document.Properties['Schedule'] = '15'
    Document.Properties['Aggregation'] = 'None'
    Document.Properties['ProbableCause'] = ''
    Document.Properties['SpecificProblem'] = ''
    Document.Properties['SelectedMeasureList'] = ''
    Document.Properties['SelectedKPI1'] = ''
    Document.Properties['SelectedKPI2'] = ''
    Document.Properties['SelectedKPI3'] = ''
    Document.Properties['SelectedKPI4'] = ''
    Document.Properties['PeriodDuration'] = ''
    Document.Properties['TableType'] = ''
    Document.Properties['DataRangeUnit'] = 'ROP'
    Document.Properties['DataRangeVal'] = '1'
    Document.Properties['LookbackUnit'] = 'ROP'
    Document.Properties['LookbackVal'] = '1'
    Document.Properties["SaveColumnsErrorMsg"] = ''
    src_table = Document.Data.Tables["Measure Mapping"]
    filt=Document.FilteringSchemes[1][src_table][src_table.Columns["Measure"]].As[ListBoxFilter]()
    filt.Reset()


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


def main():
    """ main function """

    alarm_name = get_alarm_name_from_table()
    navigate_to_page()

    if Document.Properties["IsEdit"] == "Edit":    
        remove_table("Alarm Formulas")

    remove_page(alarm_name)
    remove_table(alarm_name)
    reset_values()

main()
