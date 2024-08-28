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
# Name    : DeleteMarkedAlarmDefinitions.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)


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


conn_string = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))


class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    KPIName = 'MeasureName'
    Condition = 'Condition'
    ThresholdValue = 'ThresholdValue'
    Severity = 'Severity'  # AlarmLevel
    AlarmState = 'AlarmState'
    NECollection = 'NECollection'  # NeList
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'


def deleteAlarmDefinitionInDB(alarmName):
    sqlTemplate = '''
DELETE FROM [dbo].[AlarmDefinitions]
      WHERE [Alarm_Name] = '%s'
'''
    print('Alarm Name = ', alarmName)
    sql = sqlTemplate % alarmName
    print('sql = %s' % sql)
    dataSourceSettings = DatabaseDataSourceSettings("System.Data.SqlClient",
                                                    conn_string, sql)
    tempTableName = 'temp'
    databaseDataSource = DatabaseDataSource(dataSourceSettings)
    Document.Data.Tables.Add(tempTableName, databaseDataSource)
    Document.Data.Tables.Remove(tempTableName)


AlarmColumn = AlarmColumn()  # Create an enum to represent column names
alarmColumns = [AlarmColumn.AlarmName, AlarmColumn.AlarmType, AlarmColumn.MeasureName, AlarmColumn.Condition, AlarmColumn.ThresholdValue,
                AlarmColumn.Severity, AlarmColumn.AlarmState, AlarmColumn.NECollection, AlarmColumn.SpecificProblem, AlarmColumn.ProbableCause]

alarmDefinitionsDataTableName = 'Alarm Definitions'
alarmDefinitionsDataTable = Document.Data.Tables[alarmDefinitionsDataTableName]
markedRowSelection = Document.ActiveMarkingSelectionReference.GetSelection(alarmDefinitionsDataTable).AsIndexSet()
cursor = DataValueCursor.CreateFormatted(alarmDefinitionsDataTable.Columns[AlarmColumn.AlarmName])

for row in alarmDefinitionsDataTable.GetRows(markedRowSelection, cursor):
    deleteAlarmDefinitionInDB(alarmName=cursor.CurrentValue)

Document.Data.Tables[alarmDefinitionsDataTableName].Refresh()
Document.Properties["DeleteBtnInput"] = "Deleted"