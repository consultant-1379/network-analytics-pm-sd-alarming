#********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2021 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : TestENMConnection.py
# Date    : 20/08/2021
# Revision: 2.0
# Purpose : Test ENM Login Credentials and add to db if correct
#
# Usage   : PM Alarm
#

import clr
import json
import System.Web
import ast
from collections import OrderedDict
from System.IO import StreamReader
from System import Uri
from System.Net import ServicePointManager, SecurityProtocolType, WebRequest, CookieContainer
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from Spotfire.Dxp.Data import *
clr.AddReference('System.Data')
from System.Data.Odbc import OdbcConnection, OdbcCommand
from System.Collections import ArrayList

clr.AddReference('System.Web.Extensions')
ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
notify = Application.GetService[NotificationService]()

connString = ""

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)

def _to_bytes(lst):
    return ''.join(map(chr, lst))


def _to_hex_digest(encrypted):
    return ''.join(map(lambda x: '%02x' % x, encrypted))


def encrypt(text, digest=True):
    '''
    Performs crypting of provided text using AES algorithm.
    If 'digest' is True hex_digest will be returned, otherwise bytes of encrypted
    data will be returned.
    
    This function is simetrical with decrypt function.
    '''
    utfEncoder    = UTF8Encoding()
    bytes         = utfEncoder.GetBytes(text)
    rm            = RijndaelManaged()
    enc_transform = rm.CreateEncryptor(_key, _vector)
    mem           = MemoryStream()
    
    cs = CryptoStream(mem, enc_transform, CryptoStreamMode.Write)
    cs.Write(bytes, 0, len(bytes))
    cs.FlushFinalBlock()
    mem.Position = 0
    encrypted = Array.CreateInstance(Byte, mem.Length)
    mem.Read(encrypted, 0, mem.Length)
    cs.Close()
        
    l = map(int, encrypted)
    return _to_hex_digest(l) if digest else _to_bytes(l)


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


def checkNetAnDbConnectionStatus():
    if Document.Properties['NetAnResponseCode'] == "OK":
        return True           
    else: 
        Document.Properties[databaseConnectionResult] = "please connect to netAn DB before"
        return False


def createRequest(url, method):
    """Function that accepts url and method (POST,PUT) and creates request, and stores session

    Arguments:
        url {Uri} -- url used to connect to ENM
        method {string} -- defines whether a POST or PUT request is needed

    Returns:
        request -- [description]
    """
    request = WebRequest.Create(url)
    request.Method = method
    request.Timeout = 50000
    request.Accept = "application/json"
    request.ContentType = "application/json"
    request.CookieContainer = CookieContainer()
    return request


def createCookies(request, urlLogin):
    """Gets cookies for current session

    Arguments:
        request {httprequest} -- current connection to enm - created by createRequest()
        urlLogin {Uri} -- url used to connect to ENM

    Returns:
        cookies -- returns current cookies for session
    """
    cookies = request.CookieContainer.GetCookies(urlLogin)
    return cookies


def inValidForENM(*args):
    """checking for values that are unacceptable characters in ENM 
    **kwargs --> takes inputs a key value pair:
    """
    response = True
    if len(args) != 0:
        for value in args:
            if '#' in value or '?' in value:
                response = False
    return response


def validateEmptyFeilds(od):
    """
    checking for values if it contains any symbols which is not accepted by ENM
    """
    response = ''

    if len(od) != 0:
        for key, value in od.items():
            if value != None: 
                if not value.strip():
                    response = key
                    break
            else:
                response = "Required Feild cannot be None"
    return response


def putRequest(request, cookies, json):
    """For the session, send json data to the server - in this case send to ENM

    Arguments:
        request {httprequest} -- current connection to enm - created by createRequest()
        cookies {cookies} -- current cookies for session
        json {string} -- JSON used to send details to ENM
    """
    request.CookieContainer.Add(cookies)
    buffer = System.Text.Encoding.ASCII.GetBytes(json)
    streamWriter = request.GetRequestStream()
    streamWriter.Write(buffer, 0, len(buffer))
    streamWriter.Close()


def getResponse(response):
    """For any request, get the response stream and then close out streams

    Arguments:
        response {httpresponse} -- result of the response of the request
    """
    stream = response.GetResponseStream()
    streamReader = StreamReader(stream)
    content = streamReader.ReadToEnd()
    response_message = json.loads(content)
    streamReader.Close()
    response.Close()
    stream.Close()
    return response_message["code"]


def saveEnmToDb(serverName, userName, password, ossid, eniq_name, connString):
    #funtion takes 5 arguments related the ENM and saves it in netAnDB
    ##new entry insert 
    ##else: update table in db
   
    if insertEnmInDB(serverName, userName, password, ossid, eniq_name, connString):
        Document.Properties['ENMResponseCode'] = "ENM Connection Setup Successful"
        refreshDataTables('EniqEnmMapping')

        
def refreshDataTables(*tableNames):
    #function takes unkown amount of table names that exist in this feature as arguments and perform refresh on the tables
    tables = ArrayList()
    for tableName in tableNames:
        tables.Add(Document.Data.Tables[tableName])
    Document.Data.Tables.Refresh(tables)


def insertEnmInDB(serverName, userName, password, ossid, eniq_name, connString):  
    sql = """INSERT into "tblENM" ("EnmUrl", "EnmUsername", "EnmPassword", "OssId", "EniqID") values('{}','{}','{}','{}', (select "EniqID" from "tblEniqDS" where "EniqName" = '{}'))  
              """.format(serverName, userName, password, ossid, eniq_name)
    return writeToDB(sql, connString)


def writeToDB(sql, connString):
    try:
        connection = OdbcConnection(connString)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command.ExecuteReader()
        connection.Close()
        return True
    except Exception as e:
        print (e.message)
        Document.Properties['ENMResponseCode'] = "** Error when saving collection"
        return False


def enmEniqConnectionInDb(serverName, eniqName, OssId):
    # function returns if the eniq enm mapping already exsits.
    eniqEnmMappingTable=Document.Data.Tables['EniqEnmMapping']
    enms = eniqEnmMappingTable.Columns['EnmUrl']
    ossId = eniqEnmMappingTable.Columns['OssId']
    eniqs = eniqEnmMappingTable.Columns['EniqName']
    enmsCursor=DataValueCursor.Create[str](enms)
    eniqCursor = DataValueCursor.Create[str](eniqs)
    ossIdCursor = DataValueCursor.Create[str](ossId)
    for row in eniqEnmMappingTable.GetRows(enmsCursor, eniqCursor, ossIdCursor):
        if eniqCursor.CurrentValue == eniqName and ossIdCursor.CurrentValue == OssId:
           return True
    return False


def ValidateErrors():
    isValid = True
    errorMessage = ""
    refreshDataTables('EniqEnmMapping')
    dp = OrderedDict()
    dp["Server Name"] = Document.Properties['ENMUrl']
    dp["ENM USERNAME"] = Document.Properties['ENMUsername']
    dp["ENM PASSWORD"] = Document.Properties['ENMPassword']
    dp["OSSID"] = Document.Properties['ENMOssId']
    dp["eniq_name"] = Document.Properties['ENIQDataSourcesDropDown']

    EmptyFields = validateEmptyFeilds(dp)
    ENMValid = inValidForENM(dp["Server Name"], dp["ENM USERNAME"], dp["ENM PASSWORD"])

    if len(EmptyFields)>0 :
        isValid = False
        errorMessage = " please provide Value for: " + str(EmptyFields)
    elif ENMValid == False:
        isValid = False
        errorMessage  += ", please remove '#' or '?' from the input fields"

    elif not checkNetAnDbConnectionStatus():
        errorMessage = "please Connect to netAn DB"
        isValid = False

    elif enmEniqConnectionInDb(dp["Server Name"], dp["eniq_name"], dp["OSSID"]):
        errorMessage = "ENM, ENIQ Connection Already Exsits"
        isValid = False

    Document.Properties['ENMResponseCode'] = errorMessage
    return isValid


def clearFeilds():
    Document.Properties['ENMUrl'] = ""
    Document.Properties['ENMUserName'] = ""
    Document.Properties['ENMPassword'] = ""
    Document.Properties['ENMOssId'] = ""


if ValidateErrors():
    # create request to server and get cookies for session
    serverName = Document.Properties['ENMUrl']
    userName = Document.Properties['ENMUserName']
    password = Document.Properties['ENMPassword']
    ossid =  Document.Properties['ENMOssId']

    eniq_name = Document.Properties['ENIQDataSourcesDropDown']
    try:
        urlLogin = Uri('https://' + serverName + "/login?IDToken1=" + userName +"&IDToken2=" + password + "" )
        intialRequest = createRequest(urlLogin, "POST")
        intialResponse = intialRequest.GetResponse()
        
        cookies = createCookies(intialRequest, urlLogin)
        response = getResponse(intialResponse)

        if response == 'PASSWORD_RESET':
            Document.Properties['ENMResponseCode'] = "Default Password Must be Reset."
        else:
            Document.Properties['ENMPassword'] = encrypt(Document.Properties['ENMPassword'])
            connString = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
            saveEnmToDb(serverName, userName, encrypt(password), ossid, eniq_name, connString)
            clearFeilds()
        
    except Exception as e:
        if '401' in e.message:
            errorMsg = "Invalid Username or Password"
        else:
            errorMsg = e.message
        Document.Properties['ENMResponseCode'] = errorMsg
