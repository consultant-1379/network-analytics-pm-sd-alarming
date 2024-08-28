# ********************************************************************
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
# Name    : CheckIfReadyToAlarm.py
# Date    : 12/05/2021
# Revision: 2.0
# Purpose :
#
# Usage   : PM Alarming
#


import clr
clr.AddReference('System.Data')
from System import Array
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System.Collections.Generic import Dictionary
from System import DateTime
from System.Collections.Generic import List
import time
import logging
import re
from Spotfire.Dxp.Data.Formatters import *
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data import DataType
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Application.Scripting import ScriptDefinition


ALARM_SCHEDULE = Document.Properties['AlarmSchedule']

tables_to_keep = ["Alarm Definitions","empty_data_table", "vwEniqEnm", "Successful_Alarms", "Failed_Alarms"] 

custom_date_format = 'yyyy-MM-dd HH:mm:ss'
FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
FORMATTER.FormatString = custom_date_format

logger = logging.getLogger('root')

current_script = 'CheckIfReadyToAlarm'
logging.basicConfig(
    format="""%(asctime)s|%(levelname)s|{current_schedule}|{current_script}|%(message)s""".format(current_schedule=ALARM_SCHEDULE,current_script=current_script),
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def timeit(method):
    """ timing decorator for functions """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) )
        else:
            print '%r  %2.2f s' % \
                  (method.__name__, (te - ts) )
        return result
    return timed


def create_data_table(dataset_text, dataset_columns, data_table_name):
    """ creates a data table using a text source """

    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine(dataset_columns + '\r\n')
    writer.Flush()

    for line in dataset_text:
        writer.WriteLine(line)

    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = "|"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)
    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(data_table_name):
        Document.Data.Tables.Remove(Document.Data.Tables[data_table_name])

    Document.Data.Tables.Add(data_table_name, fs)

    # Convert any columns to the appropriate formatter
    for column in Document.Data.Tables[data_table_name].Columns:
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            column.Properties.DataType == DataType.Date
            column.Properties.Formatter = FORMATTER


def create_cursor(table):
    """Create cursors for a given table, these are used to loop through columns"""
    
    curs_list = []
    col_list = []

    for column in table.Columns:
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name], FORMATTER))
            col_list.append(table.Columns[column.Name].ToString())
        else:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name]))
            col_list.append(table.Columns[column.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict


def get_spotfire_dt(source_data_table_name, filtercondition):
    """ for a given spotfire table return the tablename, cursor and indexset """

    source_data_table = Document.Data.Tables[source_data_table_name]
    source_cur = create_cursor(source_data_table)
    
    if filtercondition != 'None':
        index_filter = Document.Data.Tables[source_data_table_name].Select(filtercondition)
        index_set = index_filter.AsIndexSet()

        return source_data_table, source_cur, index_set

    return source_data_table, source_cur


''' delete this function not in use
def getEniq():
    #returns a list of Eniq from mapping table
    table=Document.Data.Tables["vwEniqEnm"]
    cursor = DataValueCursor.CreateFormatted(table.Columns["EniqName"])
    valData = []
    for row in table.GetRows(cursor):
	    value = cursor.CurrentValue
	    if value <> str.Empty:
		    valData.append(value)
    return valData 
'''


def get_dc_z_alarm_info(curr_alarm, date_field, eniq_name):
    """ checks if an alarm is dc_z_alarm_info, and then if so return max_date"""

    filtercondition = """[AlarmName] = '{curr_alarm}' """.format(curr_alarm = curr_alarm)
    for tabl in Document.Data.Tables:
        if re.search('DC_Z_ALARM.*?_RAW_' + eniq_name,tabl.Name):
            dc_z_table = tabl.Name
    dc_z_alarm_info_dt, dc_z_alarm_info_cursor, dc_z_alarm_info_index = get_spotfire_dt(dc_z_table, filtercondition)
    for selectedmeasure in dc_z_alarm_info_dt.GetRows(dc_z_alarm_info_index, Array[DataValueCursor](dc_z_alarm_info_cursor.values())):
        max_date = dc_z_alarm_info_cursor[date_field].CurrentValue
        previous_alarm_found = True

        return previous_alarm_found, max_date
    return False, "No Date Found"


def get_log_load_date(curr_table, eniq_name):
    """ check the log_load table to get previous datetime"""
    filtercondition = ""
    log_load_date = "NULL"
    for table in curr_table:
        if table == curr_table[-1]:
            filtercondition += """[typename] = '{table}' """.format(table=table)
        else:
            filtercondition += ("""[typename] = '{table}' """.format(table=table) + " OR ")
    log_load_tablename = 'Log_LoadStatus_'+eniq_name
    log_load_dt, log_load_cursor, log_load_index = get_spotfire_dt(log_load_tablename, filtercondition)

    for _ in log_load_dt.GetRows(log_load_index, Array[DataValueCursor](log_load_cursor.values())):
        log_load_date = log_load_cursor['previous_datatime'].CurrentValue
    return log_load_date


def check_alarm_can_run(curr_alarm, curr_table, date_field, eniq_name):

    log_load_date = get_log_load_date(curr_table, eniq_name)
    alarm_triggered_before, dc_z_max_date = get_dc_z_alarm_info(curr_alarm,date_field, eniq_name)
    
    logger.info("""
        alarm_name: {curr_alarm}
        curr_table:{curr_table}
        log_load_date_previous: {log_load_date}
        dc_z_max_date:{dc_z_max_date}
        alarm_triggered_before:{alarm_triggered_before}
    """.format(curr_alarm=curr_alarm,curr_table=(",").join(curr_table),log_load_date=log_load_date,dc_z_max_date=dc_z_max_date,alarm_triggered_before=alarm_triggered_before))

    if alarm_triggered_before and (log_load_date == dc_z_max_date):
        logger.info("Alarm date in dc_z matches log_load")
        return False
    else:
        if ALARM_SCHEDULE == '60' and '00:00' not in log_load_date:
            logger.info("Hourly alarm check: 00:00 not in log load")
            return False
        else:
            logger.info("Alarm can be triggered")
            return True


def clear_tables(table_names):
    for table in Document.Data.Tables:
        if table.Name not in table_names:
            Document.Data.Tables.Remove(Document.Data.Tables[table.Name])


def remove_table_extension(table_name):
    """Removes PM data table sufix 

    Arguments:
        table_name {string} -- PM Data table name
    Returns:
        table_name {string} -- PM Data table name with removed sufix
    """

    return table_name.replace('_RAW', '').replace('_DELTA', '').replace('_DAY', '')


def main():
    """ run main function to check if alarm can be generated """

    alarm_def_dt_name = 'Alarm Definitions'
    ready_for_alarming_table_name = 'Ready For Alarming'
    alarm_def_dt, alarm_def_cursor = get_spotfire_dt(alarm_def_dt_name, 'None')

    alarms_ready_for_alarming=[]
    active_alarm_count = 0

    # clear out the ready for alarming table always
    if Document.Data.Tables.Contains(ready_for_alarming_table_name):
        Document.Data.Tables.Remove(Document.Data.Tables[ready_for_alarming_table_name])
    
    logger.info("Checking if alarms can be ran...")
    for selectedmeasure in alarm_def_dt.GetRows(Array[DataValueCursor](alarm_def_cursor.values())):
        try:
            curr_alarm = alarm_def_cursor['AlarmName'].CurrentValue
            curr_table = remove_table_extension(alarm_def_cursor['TableName'].CurrentValue)
            curr_eniq = alarm_def_cursor['EniqName'].CurrentValue
            curr_table = curr_table.split(',')
            
            active_alarm_count += 1

            date_field = 'Max_DateTime'

            can_run = check_alarm_can_run(curr_alarm, curr_table, date_field, curr_eniq)
        except Exception as e:
            logger.error("Failure checking alarm can run for alarm name: " + curr_alarm)
            logger.error(e.message)

        if can_run:
            alarms_ready_for_alarming.append(curr_alarm)

    if alarms_ready_for_alarming:
        try:
            columns = 'AlarmName'
            create_data_table(alarms_ready_for_alarming, columns, ready_for_alarming_table_name)
            logger.info("""'Ready for Alarming' table created. {alarm_ready_count} out of {active_alarm_count} alarms will be alarmed.""".format(alarm_ready_count=str(len(alarms_ready_for_alarming)),active_alarm_count=str(active_alarm_count)))

            # call script to pull fetch data
            Document.Properties["RefreshFetchData"] = DateTime.UtcNow
        except Exception as e:
            logger.error("Error creating 'Ready for Alarming' table")
            logger.error(e.message)                     
            clear_tables(tables_to_keep)
    else:
        logger.info("No alarms ready for alarming. Finished worker run.")        
        clear_tables(tables_to_keep)

main()
