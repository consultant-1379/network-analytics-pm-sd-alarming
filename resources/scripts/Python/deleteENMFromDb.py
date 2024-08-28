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
# Name    : AddSelectedNodes.py
# Date    : 17/09/2020
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
from System import Environment, Threading
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataType
from System.Data.Odbc import OdbcConnection, OdbcCommand
from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

username = Threading.Thread.CurrentPrincipal.Identity.Name

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
        notify.AddWarningNotification("Exception","something went wrong",str(e))
      

try:
    connString = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))


     

def deleteENM(enmUrl, ossId, eniqName):
    #creates query for enm deletion
    sql = """DELETE FROM "tblENM" WHERE "EnmUrl" = '{}' and "OssId" = '{}' and "EniqID" = (select "EniqID" from "tblEniqDS" where "EniqName" = '{}');""".format(enmUrl, ossId, eniqName)
    return (writeToDB(sql))

def writeToDB(sql):
    try:
        connection = OdbcConnection(connString)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command.ExecuteReader()
        connection.Close()
        return True
    except Exception as e:
        Document.Properties['ConnectionError'] = "** Error when saving collection"
        return False


def getMarkedEnm():
    # Retrieves the marked enm
  
    markedEnm = {}
    dataTable = Document.Data.Tables["EniqEnmMapping"]
    cursorENM = DataValueCursor.CreateFormatted(dataTable.Columns["EnmUrl"])
    cursorOssId = DataValueCursor.CreateFormatted(dataTable.Columns["OssId"])
    cursorEniqName = DataValueCursor.CreateFormatted(dataTable.Columns["EniqName"])

   
    markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)
    for row in dataTable.GetRows(markings.AsIndexSet(),cursorENM, cursorOssId, cursorEniqName):
        markedEnm["enm"] = cursorENM.CurrentValue
        markedEnm["ossId"] = cursorOssId.CurrentValue
        markedEnm["eniq"] = cursorEniqName.CurrentValue
    return markedEnm 



def isValid():
    if len(getMarkedEnm()) != 3:
        Document.Properties['EnmToDelete'] = "Please Select one ENM"
        return False
    else:
        return True



if isValid():
    try:
        markedEnm  = getMarkedEnm()
        if deleteENM(markedEnm['enm'], markedEnm['ossId'], markedEnm['eniq']):
            Document.Data.Tables['EniqEnmMapping'].Refresh()
            Document.Properties['ENMResponseCode'] = ""
        else:
            notify.AddWarningNotification("Exception","cannot delete selection for", Document.Properties['EnmToDelete'])
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        


