import json
from datetime import datetime, timedelta
import logging
import requests
import os
from os import listdir
from os.path import isfile, join

mypath = '/home/shared/netanplugin/netanserver/'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

pma_files = sorted([filename for filename in onlyfiles if filename.startswith("PMA_")], reverse=True) #sorted the PMA to take the lastest one
#for now its only reading first file, need to update to take most recent PMA_ file
if pma_files:
    try:
        file_path = pma_files[0]
        file = open(file_path)
        alarm_data = json.load(file)
        file.close()
    # except json.decoder.JSONDecodeError:
    except ValueError:
        # PMA file empty, could be normal as no alarming
        exit()
else:
    # No PMA json files, exit.
    exit()


logger = logging.getLogger('root')
current_script = 'exportToENM'

logging.basicConfig(
    format="""%(asctime)s|%(levelname)s|{current_schedule}|{current_script}|%(message)s""".format(current_schedule=15,current_script=current_script),
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

no_of_alarms = len(alarm_data)


def raiseAlarm(sp, pc, _logger, severity="MAJOR", eventType="ProcessingError", recordType="ALARM", manObj="", message="", FDN=""):

    headers = {
        'Content-Type': 'application/json',
    }
    data = { "specificProblem" : sp, "probableCause" : pc, "eventType" : "ProcessingError", "perceivedSeverity" : severity, "recordType" : recordType}

    if manObj != "":
        data["managedObjectInstance"] = manObj

    data["additionalAttributes"] = {}
    if message != "":
        data["additionalAttributes"]["additionalText"] = message

    if FDN != "":
        data["additionalAttributes"]["fdn"] = FDN
        data["additionalAttributes"]["behalf"] = "NetAn PMA"

    _logger.warn("Sending alarm")
    _logger.warn(data)
    try:
        response = requests.post('http://haproxy-int:8081/internal-alarm-service/internalalarm/internalalarmservice/translate', headers=headers, data=json.dumps(data))
        if "200" in str(response):
            _logger.info("Alarm sent")
        else:
            _logger.error("Failed to send Alarm!: " + str(response))
    except requests.exceptions.ConnectionError as e:
        _logger.error("Failed to send alarm!")
        _logger.error(e)


def remove_json_file():
    try:
        os.remove(file_path)
    except:
        # raise alarm
        raiseAlarm('PMA remove json file failure', 'file path error', logger, recordType="ALARM",
                   manObj='PMA module', message="")



def send_alarms():
    
    successfull_alarms_sent = 0
    for key, value in alarm_data.items():
        head, sep, tail = key.partition('_row_')
        alarm_name = head
        managedObjectInstance = alarm_data[key]["managedObjectInstance"]
        perceivedSeverity = alarm_data[key]["perceivedSeverity"]
        specificProblem = alarm_data[key]["specificProblem"]
        eventType = alarm_data[key]["eventType"]
        probableCause = alarm_data[key]["probableCause"]
        recordType = alarm_data[key]["recordType"]
        fdn = alarm_data[key]["additionalAttributes"]["fdn"]
        behalf = alarm_data[key]["additionalAttributes"]["behalf"]
        if eventType == '':
            eventType = 'ProcessingError'
        try:
            raiseAlarm(specificProblem, probableCause, logger, perceivedSeverity, eventType, recordType="ALARM", manObj=managedObjectInstance, message="", FDN=fdn)
            successfull_alarms_sent += 1
            logger.info("Alarm Success|"+ alarm_name+"|" + perceivedSeverity +"|"+ specificProblem )
        except Exception as e:
            print(str(e))
            logger.error("Alarm Fail|"+ alarm_name+"|" + perceivedSeverity +"|"+ specificProblem + "|" +str(e))


def main():
    try:
        send_alarms()
    except Exception as e:
        logger.error("Failed to send alarms")
        logger.error(e)
    finally:
        remove_json_file()

main()
