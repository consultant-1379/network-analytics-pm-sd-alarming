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
# Name    : PassAlarmState.py
# Date    : 03/12/2021
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#
from System import Array
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings
from Spotfire.Dxp.Application.Visuals import HtmlTextArea


class AlarmState:
    Active = 'Active'
    Inactive = 'Inactive'


class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'MeasureName'
    Severity = 'Severity'  # AlarmLevel
    AlarmState = 'AlarmState'
    NECollection = 'NECollection'  # NeList
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'
    Schedule = 'Schedule'
    Aggregation = 'Aggregation'
    MeasureType = 'MeasureType'


AlarmState = AlarmState()  # Create an enum to represent alarm states
AlarmColumn = AlarmColumn()  # Create an enum to represent column names
alarmColumns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName, 
                AlarmColumn.Severity, AlarmColumn.NECollection, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause,
                AlarmColumn.Schedule, AlarmColumn.Aggregation, AlarmColumn.MeasureType, AlarmColumn.AlarmState]


alarmDefinitionsDataTableName = 'Alarm Definitions'
dataTable = Document.Data.Tables[alarmDefinitionsDataTableName]


try:
    vis.As[HtmlTextArea]().HtmlContent += " "
    markedRowSelection = Document.ActiveMarkingSelectionReference.GetSelection(dataTable).AsIndexSet()

    cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in alarmColumns}


    for row in dataTable.GetRows(markedRowSelection, Array[DataValueCursor](cursors.values())):
        Document.Properties["ErrorInput"] = cursors[AlarmColumn.AlarmState].CurrentValue  


    if markedRowSelection.Count == 0 or markedRowSelection.Count > 1:
        Document.Properties["IsMarked"] = False
        Document.Properties["MarkingError"] = "*Please select only one alarm"
        
    else:
        Document.Properties["IsMarked"] = True
        Document.Properties["MarkingError"] = ""

    if markedRowSelection.Count > 0:
        Document.Properties['ExportCount'] = True
    else:
        Document.Properties['ExportCount'] = False


except AttributeError:
    print "NoneType error"


print Document.Properties["MarkingError"]