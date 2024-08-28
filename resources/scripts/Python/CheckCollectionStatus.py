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
# Name    : CheckCollectionStatus.py
# Date    : 30/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#

import clr
clr.AddReference('System.Data')
from System import Environment, Threading
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings
from System.Collections.Generic import List
from Spotfire.Dxp.Application.Visuals import HtmlTextArea
from System import Array, String
from System.Data.Odbc import OdbcConnection, OdbcCommand
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast


dataTable = Document.Data.Tables["NodeCollection"]
NetAnDB = Document.Properties["NetAnDB"]
NetAnUserName = Document.Properties["NetAnUserName"]
NetAnPassword = Document.Properties["NetAnPassword"]
DatabaseName = "netanserver_db"

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


#this script runs when the analysis is open, so if connection details arent present yet, throws an error

if Document.Properties['ConnStringNetAnDB'] == "":
    conn_string = ""
else:
    conn_string = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties["NetAnPassword"]))
    conn_string = conn_string.replace("netAnServer_pmdb","netanserver_db")

def selectedCollectionToModify():
    try:
        dataTable = Document.Data.Tables["NodeCollection"]
        cursor = DataValueCursor.CreateFormatted(dataTable.Columns["CollectionName"])
        markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)
        
        if markings.IncludedRowCount != 1:
            Document.Properties["ErrorMessage"] = "To Edit a Collection, select one collection"
            return None
        else:
            Document.Properties["ErrorMessage"] = ""	
            markedata = List [str]();
            for row in dataTable.GetRows(markings.AsIndexSet(),cursor):
                value1 = cursor.CurrentValue
            if value1 <> str.Empty:
                markedata.Add(value1)
                return markedata[0]
    except AttributeError:
        print "NoneType error"
        return None


def checkAlarmForCollection(collection_name):
    activeCollection = alarmDefinitionsDataTable.Select("[NECollection] ='" + collection_name + "'")
    return activeCollection.IsEmpty


def createCursor(eTable):
    CursList = []
    ColList = []
    colname = []
    for eColumn in eTable.Columns:
        CursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        ColList.append(eTable.Columns[eColumn.Name].ToString())
    CursArray = Array[DataValueCursor](CursList)
    cusrDict = dict(zip(ColList, CursList))
    return cusrDict 


#check collection creates dby user and who selected it. 
username = Threading.Thread.CurrentPrincipal.Identity.Name
nodeCollectionsCur = createCursor(dataTable)


def isAdmin():
    netAnDB = Document.Properties["NetAnDB"]
    sql = """
             select count(user_name) from users users inner join group_members gm on users.user_id = gm.member_user_id
             inner join groups gr on gm.group_id = gr.group_id where gr.group_name ='Administrator' and user_name = '%s'"""
    sql = sql % username

    if NetAnDB != "" and DatabaseName != "" and NetAnUserName != "" and NetAnPassword != "":

        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        reader = command.ExecuteReader()
        loopguard = 0
        while reader.Read() and loopguard != 1:  #! adding loop guard as more testing requried if multiple would be returned or while loop would break?
            adminStatus = reader[0]
            loopguard = 1
        connection.Close()    
        if adminStatus == 0:
            isAdmin = False
        else:
            isAdmin = True         
    else: 
        print "Some of the values are empty"
        isAdmin = False
    return isAdmin


def verifyUserPermission():
    CollectionName = selectedCollectionToModify()
    UserPermission = False
    for collection in dataTable.GetRows(Array[DataValueCursor](nodeCollectionsCur.values())):
        if CollectionName == nodeCollectionsCur['CollectionName'].CurrentValue and username == nodeCollectionsCur['CreatedBy'].CurrentValue: 
            UserPermission = True
            break;
    return UserPermission        

if selectedCollectionToModify() != None:
    collectionName = selectedCollectionToModify()
    if verifyUserPermission() or isAdmin(): 
        alarmDefinitionsDataTableName = 'Alarm Definitions'
        alarmDefinitionsDataTable = Document.Data.Tables[alarmDefinitionsDataTableName]
        Document.Properties["DeleteCollectionsError"] = checkAlarmForCollection(collectionName)
        Document.Properties["CheckIsMarked"] = "Marked"
        vis.As[HtmlTextArea]().HtmlContent += " "
    else:
        Document.Properties["CheckIsMarked"] = "None"
        vis.As[HtmlTextArea]().HtmlContent += " "
else:
    Document.Properties["CheckIsMarked"] = "None"
    vis.As[HtmlTextArea]().HtmlContent += " "
