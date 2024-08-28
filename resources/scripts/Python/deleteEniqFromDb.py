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
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data import DataType
from System.Data import DataSet
from System.Data.Odbc import OdbcConnection, OdbcCommand,OdbcDataAdapter
from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast
import sys

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
        



def deleteEniq(eniqName):
    sql = """DELETE FROM "tblEniqDS" WHERE "EniqName" = '{}';""".format(eniqName)
    return writeToDB(sql)

def getEniqForReports():
    sql = """SELECT string_agg(distinct "ENIQName", ',') FROM "tblSavedReports" where "ENIQName" is not null and "ENIQName" != ''"""
    return runQuery(sql)
    

def runQuery(sql):
    try:
        connection = OdbcConnection(connString)
        dataSet = DataSet()
        connection.Open()
        adaptor = OdbcDataAdapter(sql, connection)
        dataSet = DataSet()
        adaptor.Fill(dataSet)
        connection.Close()
               
        return dataSet
    except Exception as e:
        print(e)
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    
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

def createCursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""
    
    customDateFormat='yyyy-MM-dd HH:mm:ss'
    formatter=DataType.DateTime.CreateLocalizedFormatter()
    formatter.FormatString = customDateFormat



def hasEniq(tableName, eniqName, enm_mapping=False):
    '''checks if the given table contains the eniqName and in case of mapping table checks if the mapping exists'''
    contains = False
    table=Document.Data.Tables[tableName] 
    rowSelection= table.Select('EniqName = "'+eniqName+'"')
    if rowSelection.IncludedRowCount == 0:
        contains = False
    

    if enm_mapping == True:
        mappingDataTable  = Document.Data.Tables['EniqEnmMapping']
        cursor = DataValueCursor.CreateFormatted(table.Columns["EnmUrl"])
        for row in table.GetRows(rowSelection.AsIndexSet(),cursor):
            if cursor.CurrentValue != "(Empty)":
                contains = True
    elif rowSelection.IncludedRowCount > 0:
        contains = True
    return contains
       



def isValid():
    '''validates the condition to delete eniq'''
    valid = True
    if Document.Properties['EniqToDelete'] == "" or Document.Properties['EniqToDelete'] == "Please select one Eniq" or Document.Properties['EniqToDelete'] == None: 
        valid = False
        return valid    
    else: 
        eniqName = Document.Properties["EniqToDelete"]
        if hasEniq("Alarm Definitions", eniqName):
            Document.Properties['DeleteLog'] = "One or more Alarm Rule(s) is using the eniq: "+ eniqName
            valid = False
            return valid           
        elif hasEniq("NodeCollection", eniqName): 
            Document.Properties['DeleteLog']= "Node Collections is using the eniq: "+ eniqName 
            valid = False
            return valid
        elif hasEniq("EniqEnmMapping", eniqName, enm_mapping=True): 
            Document.Properties['DeleteLog']=  "One or more ENM is using the eniq: "+ eniqName 
            valid = False
            return valid
        else:
            dataset = getEniqForReports()
            eniq_present = [row[0] for row in dataset.Tables[0].Rows][0]
            #print set(eniq_present.split(','))
            if eniqName in set(eniq_present.split(',')):
                Document.Properties['DeleteLog']=  "One or more Report(s) is using the eniq: "+ eniqName 
                valid = False
                return valid

    return valid 
  


def deleteMeasureMapping(eniqName):
    table=Document.Data.Tables["Measure Mapping"]
    rowSelection=table.Select('DataSourceName = "'+eniqName+'"')
    table.RemoveRows(rowSelection)


if isValid():
    try:
        if deleteEniq(Document.Properties['EniqToDelete']):
            Document.Data.Tables['EniqEnmMapping'].Refresh()
            Document.Properties['ENMResponseCode'] = ""
            deleteMeasureMapping(Document.Properties['EniqToDelete'])
        else:
            notify.AddWarningNotification("Exception","cannot delete selection for", Document.Properties['EniqToDelete'])
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
    
        


