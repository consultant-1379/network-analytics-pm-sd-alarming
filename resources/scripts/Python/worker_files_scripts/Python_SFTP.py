import paramiko
import json
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime


local_file_path = "C:/NetAn_PMA_Exports/"+json_file_name
alarm_names_success = []
severity_success = []
specific_problem_success = []
alarm_names_fail= []
severity_fail = []
specific_problem_fail =  []
error_details_fail = []
alarm_names_success.append("placeholder")
severity_success.append("placeholder")
specific_problem_success.append("placeholder")
alarm_names_fail.append("placeholder")
severity_fail.append("placeholder")
specific_problem_fail.append("placeholder")
error_details_fail.append("placeholder")
ftp_client = None
ssh_client = None

try:
    # create ssh client 
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_client.connect(hostname='<hostname>', username='<username>', password='<password>')
    localpath = local_file_path
    
    remotepath = '/home/shared/netanplugin/netanserver/'+json_file_name
    ftp_client = ssh_client.open_sftp()
    ftp_client.put(localpath,remotepath)

    stdin, stdout, stderr = ssh_client.exec_command('(cd /home/shared/netanplugin/netanserver; python exportToENM.py)')
    log_output = stderr.readlines()
    
    for alarm in log_output:
        if "Alarm Success" in alarm:
            #log example: "Alarm Success|test_alarm_01|MINOR|Bad problem"
            alarm_details = alarm.split("|")
            alarm_name = alarm_details[1]
            severity = alarm_details[2]
            specific_problem = alarm_details[3]
            alarm_names_success.append(alarm_name)
            severity_success.append(severity)
            specific_problem_success.append(specific_problem)


        elif "Alarm Fail" in alarm:
            #log example: "Alarm Fail|test_alarm_01|MINOR|Bad problem|error"
            alarm_details = alarm.split("|")
            alarm_name = alarm_details[1]
            severity = alarm_details[2]
            specific_problem = alarm_details[3]
            error_details = alarm_details[4]
            alarm_names_fail.append(alarm_name)
            severity_fail.append(severity)
            specific_problem_fail.append(specific_problem)
            error_details_fail.append(error_details)


    alarm_success_table_columns = {
        'AlarmName':alarm_names_success,
        'PerceivedSeverityText':severity_success,
        'AdditionalText':specific_problem_success
    }

    alarm_fail_table_columns = {
        'AlarmName':alarm_names_fail,
        'PerceivedSeverityText':severity_fail,
        'AdditionalText':specific_problem_fail,
        'ErrorDetails':error_details_fail
    }

    Successful_Alarms = alarm_success_table_columns
    Failed_Alarms = alarm_fail_table_columns
    trigger_script = str(datetime.now())

except Exception as e:
    file = open(local_file_path)
    alarm_data = json.load(file)
    file.close()
    for key, value in alarm_data.items():
        head, sep, tail = key.partition('_row_')
        alarm_name = head
        severity = alarm_data[key]["perceivedSeverity"]
        specific_problem = alarm_data[key]["specificProblem"]
        error_details = str(e)
        alarm_names_fail.append(alarm_name)
        severity_fail.append(severity)
        specific_problem_fail.append(specific_problem)
        error_details_fail.append(error_details)


    alarm_success_table_columns = {
        'AlarmName':alarm_names_success,
        'PerceivedSeverityText':severity_success,
        'AdditionalText':specific_problem_success
    }

    alarm_fail_table_columns = {
        'AlarmName':alarm_names_fail,
        'PerceivedSeverityText':severity_fail,
        'AdditionalText':specific_problem_fail,
        'ErrorDetails':error_details_fail
    }
    
    Successful_Alarms = alarm_success_table_columns
    Failed_Alarms = alarm_fail_table_columns
    trigger_script = str(datetime.now())

finally:
    if ftp_client:
        ftp_client.close()
    if ssh_client:
        ssh_client.close()
    os.remove(local_file_path)
