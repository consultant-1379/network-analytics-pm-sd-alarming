
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
# Name    : TestNetAnDb.py
# Date    : 30/09/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#


import clr
clr.AddReference('System.Data')
import ast
from collections import OrderedDict
from System.Data.Odbc import OdbcConnection
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode

NetAnDB = Document.Properties["NetAnDB"]
NetAnUserName = Document.Properties["NetAnUserName"]
NetAnPassword = Document.Properties["NetAnPassword"]
DatabaseName = "netAnServer_pmdb"
MSSQL_DRIVER_LIST = [
    "{SQL Server}",
    "{ODBC Driver 13 for SQL Server}",
    "{ODBC Driver 17 for SQL Server}",    
    "{ODBC Driver 11 for SQL Server}"
]

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
                response = "Required Field cannot be None"
    return response


def validateErrors():
    isValid = True
    errorMessage = ""     
    dp = OrderedDict()
    dp["NetAn SQL DB URL"] = NetAnDB 
    dp["NetAn User Name"] = NetAnUserName
    dp["NetAn Password "] = NetAnPassword
    EmptyFields = validateEmptyFeilds(dp)
    if len(EmptyFields)>0 :
        isValid = False
        errorMessage = " please provide Value for: " + str(EmptyFields)
    Document.Properties["NetAnResponseCode"] = errorMessage
    return isValid


def get_connection_string_driver():
    try:
        conn_string = "Driver={PostgreSQL Unicode(x64)};Server="+NetAnDB+";Port=5432;Database="+DatabaseName+";Uid="+NetAnUserName+";Pwd="+NetAnPassword+";" 
        testNetAnDbConn(conn_string)
        return "Driver={PostgreSQL Unicode(x64)};Server="+NetAnDB+";Port=5432;Database="+DatabaseName+";Uid="+NetAnUserName+";Pwd=@NetAnPassword;" 
    except:
        for driver_name in MSSQL_DRIVER_LIST:
            try:
                test_connection_string = """Driver={driver_name};Server={NetAnDB};Database={DatabaseName};UID={NetAnUserName};PWD={NetAnPassword};"""
                conn_string = test_connection_string.format(driver_name=driver_name, NetAnDB=NetAnDB,DatabaseName=DatabaseName,NetAnUserName=NetAnUserName,NetAnPassword=NetAnPassword)
                testNetAnDbConn(conn_string)
                print ("Connected using driver: ", driver_name)
                return test_connection_string.format(driver_name=driver_name, NetAnDB=NetAnDB,DatabaseName=DatabaseName,NetAnUserName=NetAnUserName,NetAnPassword="@NetAnPassword")

            except Exception as e:
                print("Error connecting with driver: " + driver_name + " Testing next driver...")           
                if "Login" in str(e.message):
                    Document.Properties["NetAnResponseCode"] = "Login Failed" 
                else:
                    Document.Properties["NetAnResponseCode"] = "Cannot Create Connection"
 

def testNetAnDbConn(conn_string):
    errorMsg = ""
    sql = "SELECT count(*) from \"tblAlarmDefinitions\""
    connection = OdbcConnection(conn_string)
    connection.Open()
    command = connection.CreateCommand()
    command.CommandText = sql
    reader = command.ExecuteReader()
    loopguard = 0
    while reader.Read() and loopguard != 1:
        errorMsg = reader[0]
        loopguard = 1
    connection.Close()
    Document.Properties["NetAnResponseCode"] = "OK"
    
if validateErrors(): 
    connection_string = get_connection_string_driver()
    Document.Properties["ConnStringNetAnDB"] = connection_string
    encrypt_netan = encrypt(Document.Properties["NetAnPassword"])
    Document.Properties["NetAnPassword"] = encrypt_netan
