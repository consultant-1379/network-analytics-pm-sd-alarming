# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2020 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : SuspendResumeMarkedAlarm.py
# Date    : 08/09/2020
# Revision: 2.0
# Purpose : changes the state of Alarm Defenitions
#
# Usage   : PM Alarms
#

# alarmState is an argument to the script
# should be a string with value of either "Active" or 'Inactive' or Delete

from datetime import date, timedelta
from System import Array
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast


_key    = ast.literal_eval(Document.Properties['valArray'])
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
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)


try:
    conn_string = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)

class AlarmState:
    Active = 'Active'
    Inactive = 'Inactive'
    Deleted = 'Deleted'


class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'MeasureName'
    Severity = 'Severity'  
    AlarmState = 'AlarmState'
    NECollection = 'NECollection'  
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'


def get_alarm_definitions(data_table_name):
    alarm_definitions = {}
    if Document.Data.Tables.Contains(data_table_name):
        data_table = Document.Data.Tables[data_table_name]
        rows = IndexSet(data_table.RowCount, True)
        cursors = {column: DataValueCursor.CreateFormatted(data_table.Columns[column]) for column in alarm_columns}
        for row in data_table.GetRows(rows, Array[DataValueCursor](cursors.values())):
            alarm_definitions.update({cursors[AlarmColumn.AlarmName].CurrentValue: tuple(cursors[column].CurrentValue for column in alarm_columns[1:])})
    return alarm_definitions



def update_alarm_definition_in_db(alarm_name, alarm_state):
    try:
        retention_length = float(Document.Properties["RetentionLength"])
    
        if alarmState == 'Deleted':
            deletion_date = str(date.today() + timedelta(retention_length))
            sql_template = '''
            UPDATE "tblAlarmDefinitions"
            SET "AlarmState" = '%s', "DeletionDate" = '''+("'"+deletion_date+"'")+''' 
            WHERE "AlarmName" = '%s';
            '''
        else:
            deletion_date = str(date.today() + timedelta(50000))
            sql_template = '''
            UPDATE "tblAlarmDefinitions"
            SET "AlarmState" = '%s', "DeletionDate" = '''+("'"+deletion_date+"'")+'''
            WHERE "AlarmName" = '%s';
            '''

        sql = sql_template % (alarm_state, alarm_name)
        data_source_settings = DatabaseDataSourceSettings("System.Data.Odbc",conn_string, sql)
        table_name = 'temp'
        data_base_data_source = DatabaseDataSource(data_source_settings)
        new_data_table = Document.Data.Tables.Add(table_name, data_base_data_source)
        Document.Data.Tables.Remove(table_name)
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)


alarm_state = AlarmState()
alarm_column = AlarmColumn()
alarm_columns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName, AlarmColumn.Severity, AlarmColumn.AlarmState, AlarmColumn.NECollection, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause]
alarm_definition_error_property_name = 'AlarmDefinitionError'
Document.Properties[alarm_definition_error_property_name] = ''
Document.Properties['ExportMessage'] = ''
marked_row_selection = ""

alarm_definitions_data_table_name = 'Alarm Definitions'
alarm_definitions = get_alarm_definitions(data_table_name=alarm_definitions_data_table_name)

data_table = Document.Data.Tables[alarm_definitions_data_table_name]

try:
    marked_row_selection = Document.ActiveMarkingSelectionReference.GetSelection(data_table).AsIndexSet()
except AttributeError:
    print "NoneType error"

cursors = {column: DataValueCursor.CreateFormatted(data_table.Columns[column]) for column in alarm_columns}

for row in data_table.GetRows(marked_row_selection, Array[DataValueCursor](cursors.values())):
    if alarmState != cursors[AlarmColumn.AlarmState].CurrentValue:
      update_alarm_definition_in_db(alarm_name=cursors[AlarmColumn.AlarmName].CurrentValue, alarm_state = alarmState)
      Document.Properties["DeleteBtnInput"] = "Deleted"

Document.Data.Tables[alarm_definitions_data_table_name].Refresh()
