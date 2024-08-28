from datetime import date, timedelta
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings
from time import sleep 
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast

# global vars
conn_string_eniq = "DSN=" + Document.Properties['ENIQDB'] + ";Pooling=true;Max Pool Size=20;Enlist=true;FetchArraySize=100000;"


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


def deleteDeadAlarmDefinitons():
    try:
        deletionDate = str(date.today())
        sqlTemplate = ''' 
        DELETE FROM "tblAlarmDefinitions"
        WHERE "DeletionDate" < '''+("'"+deletionDate+"'")+'''  '''
        sql = sqlTemplate
        #print('sql = %s' % sql)
        dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc",conn_string, sql)
        tableName = 'temp'
        databaseDataSource = DatabaseDataSource(dataSourceSettings)
        newDataTable = Document.Data.Tables.Add(tableName, databaseDataSource)
        Document.Data.Tables.Remove(tableName)
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)


deleteDeadAlarmDefinitons()
alarmDefinitionsDataTableName = 'Alarm Definitions'
Document.Data.Tables[alarmDefinitionsDataTableName].Refresh()

Document.Properties["ErrorInput"] = "Refreshed"

