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
# Name    : DeleteCollection.py
# Date    : 04/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Explorer
#

from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings
from System.Collections.Generic import List
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

CollectionName = Document.Properties['SelectedCollectionToModify']
CollectionTable = Document.Data.Tables['NodeCollection']
cursor = DataValueCursor.CreateFormatted(CollectionTable.Columns["CollectionName"])
alarmDefinitionsDataTableName = 'Alarm Definitions'
alarmDefinitionsDataTable = Document.Data.Tables[alarmDefinitionsDataTableName]


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

def selectedCollectionToModify():
    dataTable = Document.Data.Tables["NodeCollection"]
    cursor = DataValueCursor.CreateFormatted(dataTable.Columns["CollectionName"])
    cursor1 = DataValueCursor.CreateFormatted(dataTable.Columns["CollectionName"])
    markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)
    
    if markings.IncludedRowCount != 1 and markings.IncludedRowCount == 0:
        return None
    else:
        Document.Properties["ErrorMessage"] = ""	
        markedata = List [str]();
        for row in dataTable.GetRows(markings.AsIndexSet(),cursor):
	    value1 = cursor.CurrentValue
	    if value1 <> str.Empty:
		    markedata.Add(value1)
            return markedata[0]


def checkAlarmForCollection(collection_name):
    activeCollection = alarmDefinitionsDataTable.Select("[NECollection] ='" + collection_name + "'")
    return activeCollection.IsEmpty


def deleteNodeCollection(collection_name):
    sqlTemplate = '''
    DELETE FROM "tblCollection"
    WHERE "CollectionName" = '%s'
    '''

    Document.Properties['ErrorMessage'] = "Node Collection Deleted"
    sql = sqlTemplate % collection_name
    dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc",
                                                    conn_string, sql)
    tempTableName = 'temp'
    databaseDataSource = DatabaseDataSource(dataSourceSettings)
    Document.Data.Tables.Add(tempTableName, databaseDataSource)
    Document.Properties["DeleteCollectionsInput"] = ""
    Document.Data.Tables.Remove(tempTableName)


try:
    if selectedCollectionToModify() != None:
        CollectionName = selectedCollectionToModify()
        if checkAlarmForCollection(CollectionName):
            deleteNodeCollection(CollectionName)
            Document.Data.Tables['NodeCollection'].Refresh()
            Document.Properties["ErrorMessage"] = "Collection Deleted"

except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)	
