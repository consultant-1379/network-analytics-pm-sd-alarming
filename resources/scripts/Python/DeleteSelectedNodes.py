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
# Name    : DeleteSelectedNodes.py
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



CollectionName = Document.Properties['SelectedCollectionToModify']
CollectionTable = Document.Data.Tables['NodeCollection']
cursor = DataValueCursor.CreateFormatted(CollectionTable.Columns["CollectionName"])
alarmDefinitionsDataTableName = 'Alarm Definitions'
alarmDefinitionsDataTable = Document.Data.Tables[alarmDefinitionsDataTableName]



def deleteNodeCollectionFromTable(Collection_name):
    for row in CollectionTable.GetRows(cursor):
        value = cursor.CurrentValue
        if CollectionName == value:
            RowSelection = CollectionTable.Select("CollectionName = '%s'" % str(value))
            CollectionTable.RemoveRows(RowSelection)


def deleteNodeCollection(collection_name):
    sqlTemplate = '''
    DELETE FROM [dbo].[NodeCollections]
    WHERE [CollectionName] = '%s'
    '''

    Document.Properties['ActionMessage'] = "Node Collection Deleted"
    sql = sqlTemplate % collection_name
    print('sql = %s' % sql)
    dataSourceSettings = DatabaseDataSourceSettings("System.Data.SqlClient",
                                                    conn_string, sql)
    tempTableName = 'temp'
    databaseDataSource = DatabaseDataSource(dataSourceSettings)
    Document.Data.Tables.Add(tempTableName, databaseDataSource)
    Document.Properties["DeleteCollectionsInput"] = ""
    Document.Data.Tables.Remove(tempTableName)


def checkAlarmForCollection(NE_Collection):
    activeCollection = alarmDefinitionsDataTable.Select("[NECollection] ='" + NE_Collection + "'")
    return activeCollection.IsEmpty


print checkAlarmForCollection(CollectionName)

if checkAlarmForCollection(CollectionName):
    deleteNodeCollection(CollectionName)
    deleteNodeCollectionFromTable(CollectionName)
    Document.Properties["selectedNodes"] = ""
    Document.Properties["DeleteCollectionsInput"] = ""
    Document.Properties["SelectedCollectionToModify"] = None
     
    Document.Data.Tables['NodeCollection'].Refresh()
    
