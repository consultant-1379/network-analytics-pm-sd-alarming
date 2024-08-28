# (c) Ericsson Inc. 2020 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : CreateAlarmDefinition.py
# Date    : 24/06/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#
import System
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextFileDataSource, TextDataReaderSettings
import clr
clr.AddReference('System.Data')
from System.Data.SqlClient import SqlConnection
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System import Array
from collections import OrderedDict
from Spotfire.Dxp.Application.Filters import ListBoxFilter
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Application.Filters import *

from System import DateTime



alarm_definitions_columns = '''AlarmId
AlarmName
AlarmType
MeasureName
Condition
ThresholdValue
Severity
SpecificProblem
ProbableCause
Schedule
Aggregation
MeasureType
AlarmState
TABLE_TYPE
NodeType
SystemArea
SingleOrCollection
NECollection
LookBack'''.split('\n')



table_name = "Alarm Definitions"
delimiter = '#'
stream = MemoryStream()
csvWriter = StreamWriter(stream)
csvWriter.WriteLine('#'.join(alarm_definitions_columns) + '\r\n')
settings = TextDataReaderSettings()
settings.Separator = delimiter
settings.AddColumnNameRow(0)
csvWriter.Flush()
stream.Seek(0, SeekOrigin.Begin)
textFileDataSource = TextFileDataSource(stream, settings)

if Document.Data.Tables.Contains(table_name):
    Document.Data.Tables[table_name].ReplaceData(textFileDataSource)



  

