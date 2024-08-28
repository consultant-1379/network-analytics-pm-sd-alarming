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
# Date    : 20/08/2021
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#

from System import Environment
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource ,TextDataReaderSettings,TextFileDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *
from System.Reflection import Assembly
from Spotfire.Dxp.Data.Collections import *
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.Runtime.Serialization import ISerializable
from System.Collections import IComparer
from System.Collections.Generic import IComparer
from Spotfire.Dxp.Application.Visuals import HtmlTextArea
from System.IO import StreamWriter, MemoryStream, SeekOrigin, FileStream, FileMode, File
from Spotfire.Dxp.Data import DataValueCursor

import clr
clr.AddReference('System.Data')
from System import Array, Byte
from System.Text import UTF8Encoding
from System.Data.Odbc import OdbcConnection, OdbcCommand
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast
import re
notify = Application.GetService[NotificationService]()


ps = Application.GetService[ProgressService]()
sql = u"select count(AGGREGATION) FROM LOG_Aggregations"

dataSourceNames = Document.Properties["ENIQDB"].replace(" ", "").split(",")
dataSourceNames = [re.sub('[^a-zA-Z0-9_ \n\.]', '', eniq) for eniq in dataSourceNames]
combinedDataSources = set()


connectionStatus = {}
databaseConnectionResult = "DatabaseConnectionResult"
ENIQDSTABLENAME = "ENIQDataSources" 
Document.Properties[databaseConnectionResult] = ""



tableName = 'Test DB Connection'


def checkNetAnDbConnectionStatus():
    if Document.Properties['NetAnResponseCode'] == "OK":
        return True
    else: 
        Document.Properties[databaseConnectionResult] = "please connect to netAn DB before connection ENIQ"
        return False


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




if Document.Properties['NetAnPassword'] != "":
    try:
        connString = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)	




def fetchDataFromENIQAsync():
    if len(dataSourceNames) > 0:
        failed_ds = "Failed to connect with: "
        for dataSourceName in combinedDataSources:
            if dataSourceName!='':
                try:
                    ps.CurrentProgress.ExecuteSubtask('Testing Connection to %s ...' % (dataSourceName))
                    ps.CurrentProgress.CheckCancel()
                    dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
                    dataTableDataSource = DatabaseDataSource(dataSourceSettings)
                    if Document.Data.Tables.Contains(tableName):      # If exists, remove it
                        Document.Data.Tables.Remove(tableName)
                    Document.Data.Tables.Add(tableName, dataTableDataSource)
                    Document.Properties[databaseConnectionResult] += ''
                    Document.Properties["ConnectionError"] = ""
                    Document.Data.Tables.Remove(tableName)
                    Document.Properties["conn"] = "Connected to: "+ dataSourceName
                    Document.Properties["ConnectedDataSource"] = dataSourceName
                    Document.Properties["DbConnectionUp"] = 'True'
                    connectionStatus[dataSourceName] = "Connected"
                except:
                    failed_ds += dataSourceName + ","
                    Document.Properties[databaseConnectionResult] = failed_ds
                    Document.Properties["conn"] += "Failed to connect to: "+ dataSourceName

    else:
        Document.Properties[databaseConnectionResult] = 'Enter Data Source Name'




def createTable(dataTableName, connectionStatus):
    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine("ENIQ_DS,ConnectionStatus\r\n")
    writer.Flush()
    for eniqDb in connectionStatus:
        row = eniqDb +","+ connectionStatus[eniqDb]
        writer.WriteLine(row)
    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = ","
    settings.AddColumnNameRow(0)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(dataTableName):
        dataTable = Document.Data.Tables[dataTableName]
        # clear table

        dataTable.ReplaceData(fs)
    else:
        # Create Table if not already present
        dataTable = Document.Data.Tables.Add(dataTableName, fs)



def insertEniqDs(EniqNames):
    values = connectedEniqDs(EniqNames)
    if len(values) > 0: 
        sql = """INSERT into "tblEniqDS" ("EniqName") Values{} 
              """.format(values)
    else:
       sql = None
    return sql  


def connectedEniqDs(newConnections):
    #returns comma seperated eniq ds in right format to be inserted in db 
    eniqDsValues = ""
    for dataSourceName in newConnections:
        if dataSourceName in connectionStatus:
            values = " ('"+ dataSourceName+"')"
            eniqDsValues = eniqDsValues + values 
    return eniqDsValues[1:].replace(" ", ",")


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
        Document.Properties['DatabaseConnectionResult'] = "** Error when saving collection"
        return False


def eniqInDb():
    # function returns set of already stored eniq_names in db
    savedEniq = list()
    eniqEnmMappingTable=Document.Data.Tables['EniqEnmMapping']
    eniqEnmMappingTable.Refresh()
    eniqs = eniqEnmMappingTable.Columns['EniqName']
    columnCursor=DataValueCursor.Create[str](eniqs)
    for row in eniqEnmMappingTable.GetRows(columnCursor):
	    if columnCursor.IsCurrentValueValid:
		    savedEniq.append(columnCursor.CurrentValue)
    return savedEniq
 


if checkNetAnDbConnectionStatus(): 
    savedEniq = eniqInDb()
    SavedEniqLowerCase  = [lower_eniq.lower() for lower_eniq in savedEniq]
    #print "SavedEniqLowerCase", SavedEniqLowerCase
    combinedDataSources = set(dataSourceNames + savedEniq)
    
    ps.ExecuteWithProgress('Testing Connection to ENIQ(s)', 'Testing Connection to ENIQ(s)', fetchDataFromENIQAsync)
    #add connected datasources to db 
    newDataSources = [eniq for eniq in connectionStatus.keys() if eniq.lower() not in SavedEniqLowerCase]
    #print "newDataSources", newDataSources
    createTable(ENIQDSTABLENAME, connectionStatus)
    Document.Properties['ENIQDB'] = ""
    if len(newDataSources) > 0:
        sql = insertEniqDs(newDataSources)
        if sql != None:
            Document.Properties[databaseConnectionResult] = "Added"
            writeToDB(sql)
            Document.Data.Tables["EniqEnmMapping"].Refresh()  
        
    
    



