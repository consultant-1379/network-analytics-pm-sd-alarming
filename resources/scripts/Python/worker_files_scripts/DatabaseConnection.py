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
# Name    : DatabaseConnection.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *
from System.Reflection import Assembly
from Spotfire.Dxp.Data.Collections import *
from System.Runtime.Serialization import ISerializable
from System.Collections import IComparer
from System.Collections.Generic import IComparer
from Spotfire.Dxp.Application.Visuals import HtmlTextArea

ps = Application.GetService[ProgressService]()
sql = u"select count(AGGREGATION) FROM LOG_Aggregations"
dataSourceName = Document.Properties["ENIQDB"]
databaseConnectionResult = "DatabaseConnectionResult"
if dataSourceName != '':
    try:
        dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
    except:
        Document.Properties[databaseConnectionResult] = 'Connection Failed'
else:
    Document.Properties[databaseConnectionResult] = 'Enter Data Source Name'
tableName = 'Test DB Connection'


def fetchDataFromENIQAsync():
  if dataSourceName!='':
    try:
        ps.CurrentProgress.ExecuteSubtask('Testing Connection to %s ...' % (dataSourceName))
        ps.CurrentProgress.CheckCancel()
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        if Document.Data.Tables.Contains(tableName):      # If exists, remove it
            Document.Data.Tables.Remove(tableName)
        Document.Data.Tables.Add(tableName, dataTableDataSource)
        Document.Properties[databaseConnectionResult] = 'OK'
        Document.Data.Tables.Remove(tableName)
    except:
        Document.Properties[databaseConnectionResult] = 'Connection Failed'

  else:
    Document.Properties[databaseConnectionResult] = 'Enter Data Source Name'


ps.ExecuteWithProgress('Testing Connection to %s ...' % (dataSourceName), 'Testing Connection to %s ...' % (dataSourceName), fetchDataFromENIQAsync)
