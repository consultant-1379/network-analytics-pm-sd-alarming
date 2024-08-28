
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
# Name    : TestENMConnection.py
# Date    : 09/04/2019
# Revision: 1.0
# Purpose : Test ENM Login Credentials
#
# Usage   : PM Alarm
#

import clr
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

from Spotfire.Dxp.Data import *

clr.AddReference('System.Web.Extensions')
ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12

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
    streamReader.Close()
    response.Close()
    stream.Close()




def ValidateErrors():
    isValid = True
    errorMessage = ""

    dp = OrderedDict()
    dp["Server Name"] = Document.Properties['ENMDB2']
    dp["ENM USERNAME"] = Document.Properties['ENMUserName2']
    dp["ENM PASSWORD"] = Document.Properties['ENMPassword2']
    dp["OSSID"] = Document.Properties['OSSID2']
    
    EmptyFields = validateEmptyFeilds(dp)
    ENMValid = inValidForENM(dp["Server Name"], dp["ENM USERNAME"], dp["ENM PASSWORD"])
    
    if len(EmptyFields)>0 :
        isValid = False
        errorMessage = " please provide Value for: " + str(EmptyFields)
    if ENMValid == False:
        isValid = False
        errorMessage  += ", please remove '#' or '?' from the input fields"
    if Document.Properties['OSSID'] == Document.Properties['OSSID2']:
        isValid = False
        errorMessage = " please provide Value for: OSSID" 
    Document.Properties['ENMResponseCode2'] = errorMessage
    return isValid



if ValidateErrors():
    # create request to server and get cookies for session
    serverName = Document.Properties['ENMDB2']
    userName = Document.Properties['ENMUserName2']
    password = Document.Properties['ENMPassword2']
    try:
        urlLogin = Uri("https://" + serverName + "/login?IDToken1=" + userName +"&IDToken2=" + password + "" )
        intialRequest = createRequest(urlLogin, "POST")
        intialResponse = intialRequest.GetResponse()

        cookies = createCookies(intialRequest, urlLogin)
        getResponse(intialResponse)
        Document.Properties['ENMResponseCode2'] = intialResponse.StatusDescription
        Document.Properties['ENMPassword2'] = encrypt(Document.Properties['ENMPassword2'])
    except Exception as e:
        if '401' in e.message:
            errorMsg = "Invalid Username or Password"
        else:
            errorMsg = e.message
        Document.Properties['ENMResponseCode2'] = errorMsg
