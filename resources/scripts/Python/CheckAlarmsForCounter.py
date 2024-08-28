# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2019 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : CheckAlarmsForCounter.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *
from Spotfire.Dxp.Data import *

ps = Application.GetService[ProgressService]()
print ps
selectedTable = Document.Properties["ListOfERBSTables"]
print selectedTable
selectedCounter = Document.Properties["ListOfCounters"]
print type(selectedCounter)
thresholdLimit = Document.Properties["ThresholdLimit"]
print type(thresholdLimit)
SelectedERBSTable = Document.Data.Tables['Counters']

sql = "select MOID,OSS_ID,DATETIME_ID,DATE_ID,YEAR_ID,MONTH_ID,DAY_ID,HOUR_ID,MIN_ID,TIMELEVEL,ERBS," + selectedCounter + " from "
+ selectedTable + " where (DATETIME_ID <= ( SELECT MAX(DATETIME_ID) FROM "
+ selectedTable + ") AND DATETIME_ID >= (SELECT DATEADD(minute, -15 , MAX(DATETIME_ID)) FROM " + selectedTable + ")) and "
+ selectedCounter + " > " + thresholdLimit + ""

print sql
dataSourceName = "ieatrcxb6511-dwh"
databaseConnectionResult = "DatabaseConnectionResult"
dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
tableName = 'Alarm Table'
print tableName

# Document.Properties[databaseConnectionResult] = 'Connection OK'


def fetchDataFromENIQAsync():
    try:
        ps.CurrentProgress.ExecuteSubtask('Testing Connection to %s ...' % (dataSourceName))
        print "Execute"
        ps.CurrentProgress.CheckCancel()
        print "Cancel"
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        print dataTableDataSource
        tableName = Document.Data.Tables["Alarm Table"]
        if Document.Data.Tables.Contains(tableName):
            print "Exists"
            print tableName.RowCount
            tableName.RemoveRows(RowSelection(IndexSet(tableName.RowCount, True)))
            print tableName.RowCount
            # Document.Data.Tables.Remove(tableName)
            # print "Removed"
        settings = AddRowsSettings(tableName, dataTableDataSource)

# Add the rows from the datasource.
    tableName.AddRows(dataTableDataSource,settings)
    print tableName.RowCount
    # print dt.Name
    # Document.Data.Tables.Remove(dt)
    # print "Removed"
    # Document.Properties[databaseConnectionResult] = 'Connection Failed'
    except:
        print "except"
        raise

ps.ExecuteWithProgress('Testing Connection to %s ...' % (dataSourceName), 'Testing Connection to %s ...' % (dataSourceName), fetchDataFromENIQAsync)
