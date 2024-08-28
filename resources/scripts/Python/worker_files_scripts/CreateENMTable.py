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
# Name    : CreateENMTable.py
# Date    : 09/12/2021
# Revision: 1.0
# Purpose :
#
# Usage   : PM Alarming
#

import time
import clr
clr.AddReference('System.Data')
from System import Array
from System.IO import StreamWriter, MemoryStream, SeekOrigin
from System import DateTime
import logging
import re
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data import DataType
from Spotfire.Dxp.Data.Import import TextFileDataSource, TextDataReaderSettings
from Spotfire.Dxp.Data.Formatters import *

# global vars
ALARM_COLUMNS = [
    'ALARM_NAME',
    'ELEMENT',
    'SN',
    'MOID',
    'DC_TIMEZONE',
    'DATETIME_ID',
    'DATE_ID',
    'HOUR_ID',
    'OSS_ID',
    'MEASUREVALUE',
    'THRESHOLDINFORMATION',
    'ObjectOfReference',
    'SEVERITY'
]

custom_date_format = 'yyyy-MM-dd HH:mm:ss'
DATETIME_FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
DATETIME_FORMATTER.FormatString = custom_date_format

custom_date_format = 'yyyy-MM-dd'
DATE_FORMATTER = DataType.DateTime.CreateLocalizedFormatter()
DATE_FORMATTER.FormatString = custom_date_format

ALARM_SCHEDULE = Document.Properties['AlarmSchedule']

logger = logging.getLogger('root')
current_script = 'CreateENMTable'
logging.basicConfig(
    format="""%(asctime)s|%(levelname)s|{current_schedule}|{current_script}|%(message)s""".format(current_schedule=ALARM_SCHEDULE,current_script=current_script),
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def timeit(method):
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


def create_cursor(table):
    """Create cursors for a given table, these are used to loop through columns"""
    
    curs_list = []
    col_list = []

    for column in table.Columns:
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name], DATETIME_FORMATTER))
            col_list.append(table.Columns[column.Name].ToString())
        else:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name]))
            col_list.append(table.Columns[column.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict


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


def remove_table_extension(table_name):
    """Removes PM data table sufix 

    Arguments:
        table_name {string} -- PM Data table name
    Returns:
        table_name {string} -- PM Data table name with removed sufix
    """

    return table_name.replace('_RAW', '').replace('_DELTA', '').replace('_DAY', '')


def get_spotfire_dt(source_data_table_name, filtercondition):
    """ for a given spotfire table return the tablename, cursor and indexset """

    source_data_table = Document.Data.Tables[source_data_table_name]
    source_cur = create_cursor(source_data_table)
    
    if filtercondition != 'None':
        index_filter = Document.Data.Tables[source_data_table_name].Select(filtercondition)
        index_set = index_filter.AsIndexSet()

        return source_data_table, source_cur, index_set

    return source_data_table, source_cur


@timeit
def create_data_table(table_name, text_data, column_names):
    """ creates a data table using a text source """
   
    stream = MemoryStream()
    writer = StreamWriter(stream)
    writer.WriteLine('|'.join(column_names) + '\r\n')
    writer.Flush()

    for line in text_data:
        writer.WriteLine(line)

    writer.Flush()
    settings = TextDataReaderSettings()
    settings.Separator = "|"
    settings.AddColumnNameRow(0)
    settings.ClearDataTypes(False)

    for i in range(len(column_names)):
        if column_names[i] == 'DATETIME_ID':
            settings.SetDataType(i, DataType.DateTime)
        elif column_names[i] == 'DATE_ID':
            settings.SetDataType(i, DataType.Date)

    stream.Seek(0, SeekOrigin.Begin)
    fs = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(table_name):
    
        dataTable = Document.Data.Tables[table_name]
        dataTable.ReplaceData(fs)
    else:
        Document.Data.Tables.Add(table_name, fs)

    # Convert any columns to the appropriate formatter
    for column in Document.Data.Tables[table_name].Columns:
        formatter = DATETIME_FORMATTER
        if column.Name == 'DATE_ID':
            formatter = DATE_FORMATTER
               
        if column.Properties.DataType.ToString() in ['DateTime', 'Date']:
            column.Properties.DataType == DataType.Date
            column.Properties.Formatter = formatter


def remove_table(source_alarm_table):
    """ remove given data table """

    if Document.Data.Tables.Contains(source_alarm_table):
            Document.Data.Tables.Remove(source_alarm_table)


def get_severity(alarm):
    filtercondition = """[AlarmName] = '{alarm}' """.format(alarm = alarm)

    alarm_info_dt, alarm_info_cursor, alarm_info_index = get_spotfire_dt("Alarm Definitions", filtercondition)
    for selectedmeasure in alarm_info_dt.GetRows(alarm_info_index, Array[DataValueCursor](alarm_info_cursor.values())):
        severity = alarm_info_cursor["Severity"].CurrentValue

        return severity

def check_alarm_or_event(alarm):
    """ checks if given alarm is an alarm or event"""

    alarm_def_dt_name = 'Alarm Definitions'
    alarm_filtercondition = "[AlarmName] = '"+alarm+"'"
    alarm_def_dt, alarm_def_cursor, alarm_index = get_spotfire_dt(alarm_def_dt_name, alarm_filtercondition)
    is_alarm = ""
    for selectedmeasure in alarm_def_dt.GetRows(alarm_index, Array[DataValueCursor](alarm_def_cursor.values())):
        is_alarm = alarm_def_cursor["AlarmOrEvent"].CurrentValue
    return is_alarm


def get_table_rows(alarm):
    """ for current alarm, get each column value in a row and append. Add to an overall list of rows and return """
    
    alarm_data_table_rows = []

    try:
        # filter for alarms where criteria is met
        filtercondition = "[ALARM_CRITERIA] = 1"
        current_alarm_table, current_alarm_cursor, current_alarm_index = get_spotfire_dt(alarm, filtercondition)
        # translation columns to take the column from the ENIQ tables and convert into the format required for ExportToENM script
        translation_columns = {
            'ALARM_NAME':alarm,
            'MEASUREVALUE':'',
            'THRESHOLDINFORMATION':''
        }

        measure_val_names = [col.Name for col in current_alarm_table.Columns if 'MEASUREVALUE_' in col.Name]
        translation_columns['THRESHOLDINFORMATION'] = '1'

        for selectedmeasure in current_alarm_table.GetRows(current_alarm_index, Array[DataValueCursor](current_alarm_cursor.values())):
            curr_row = []
            
            translation_columns['MEASUREVALUE'] = ';'.join([current_alarm_cursor[col_name].CurrentValue for col_name in measure_val_names])
            
            for column in ALARM_COLUMNS:
                # if the column needs to be translated
                if translation_columns.get(column):
                    col_value = translation_columns[column]
                else:
                    # if doesnt need to be translated, get the value directly from the table
                    try:
                        col_value = current_alarm_cursor[column].CurrentValue
                    except Exception as e:
                        # no value found so placing empty value
                        col_value = ''
                if column == "SEVERITY":
                    col_value = get_severity(alarm)
                curr_row.append(col_value)
                
            alarm_data_table_rows.append("%s\r\n" % ('|'.join(curr_row)))

        logger.info("""{row_count} row(s) met alarm criteria for alarm: {alarm}.""".format(row_count=len(alarm_data_table_rows), alarm=alarm))

        # once data is loaded into text array, delete out the table
        remove_table(current_alarm_table)

        return alarm_data_table_rows

    except Exception as e:
        logger.error("""Error reading alarm criteria for alarm: {0}.""".format(alarm))
        remove_table(alarm)
        return []


def get_table_rows_alarm(alarm):
    """ for current alarm, get each column value in a row and append. Add to an overall list of rows and return """

    alarm_data_table_rows = []


    # filter for alarms where criteria is met
    filtercondition_alarm = "[ALARM_CRITERIA] = 1"
    current_alarm_table_alarm, current_alarm_cursor_alarm, current_alarm_index_alarm = get_spotfire_dt(alarm, filtercondition_alarm)
    
    # filter for alarms where clear condition is met
    filtercondition_clear = "[CLEAR_CONDITION] = 1"
    current_alarm_table_clear, current_alarm_cursor_clear, current_alarm_index_clear = get_spotfire_dt(alarm, filtercondition_clear)

    # translation columns to take the column from the ENIQ tables and convert into the format required for ExportToENM script
    translation_columns = {
        'ALARM_NAME':alarm,
        'MEASUREVALUE':'',
        'THRESHOLDINFORMATION':''
    }

    measure_val_names = [col.Name for col in current_alarm_table_alarm.Columns if 'MEASUREVALUE_' in col.Name]
    translation_columns['THRESHOLDINFORMATION'] = '1'


    ##### FOR ALARM CONDITION #########
    for selectedmeasure in current_alarm_table_alarm.GetRows(current_alarm_index_alarm, Array[DataValueCursor](current_alarm_cursor_alarm.values())):
        curr_row = []
        
        translation_columns['MEASUREVALUE'] = ';'.join([current_alarm_cursor_alarm[col_name].CurrentValue for col_name in measure_val_names])
        
        for column in ALARM_COLUMNS:
            # if the column needs to be translated
            if translation_columns.get(column):
                col_value = translation_columns[column]
            else:
                # if doesnt need to be translated, get the value directly from the table
                try:
                    col_value = current_alarm_cursor_alarm[column].CurrentValue
                except Exception as e:
                    # no value found so placing empty value
                    col_value = ''
            if column == "SEVERITY":
                col_value = get_severity(alarm)
            curr_row.append(col_value)
            
        alarm_data_table_rows.append("%s\r\n" % ('|'.join(curr_row)))

    ##### FOR CLEAR CONDITION #########
    for selectedmeasure in current_alarm_table_clear.GetRows(current_alarm_index_clear, Array[DataValueCursor](current_alarm_cursor_clear.values())):
        curr_row = []
        
        translation_columns['MEASUREVALUE'] = ';'.join([current_alarm_cursor_clear[col_name].CurrentValue for col_name in measure_val_names])
        
        for column in ALARM_COLUMNS:
            # if the column needs to be translated
            if translation_columns.get(column):
                col_value = translation_columns[column]
            else:
                # if doesnt need to be translated, get the value directly from the table
                try:
                    col_value = current_alarm_cursor_clear[column].CurrentValue
                except Exception as e:
                    # no value found so placing empty value
                    col_value = ''
            if column == "SEVERITY":
                col_value = "CLEAR"

            curr_row.append(col_value)

        alarm_def_dt, alarm_def_cursor = get_spotfire_dt('Alarm Definitions', 'None')
        for selectedmeasure in alarm_def_dt.GetRows(Array[DataValueCursor](alarm_def_cursor.values())):
            try:
                curr_alarm = alarm_def_cursor['AlarmName'].CurrentValue
                curr_table = remove_table_extension(alarm_def_cursor['TableName'].CurrentValue)
                curr_eniq = alarm_def_cursor['EniqName'].CurrentValue
                curr_table = curr_table.split(',')
            except Exception as e:
                logger.error("Failure checking alarm can run for alarm name: " + curr_alarm)
                logger.error(e.message)


        alarmed,DC_Z_MAX_Date=get_dc_z_alarm_info(alarm, "Max_DateTime", curr_eniq)
        log_load_date=get_log_load_date(curr_table, curr_eniq)

        if alarmed:
            alarm_data_table_rows.append("%s\r\n" % ('|'.join(curr_row)))

    logger.info("""{row_count} row(s) met alarm criteria for alarm: {alarm}.""".format(row_count=len(alarm_data_table_rows), alarm=alarm))

    # once data is loaded into text array, delete out the table
    remove_table(current_alarm_table_alarm)
    remove_table(current_alarm_table_clear)

    return alarm_data_table_rows


def clear_tables(table_names):
    for table in Document.Data.Tables:
        if table.Name not in table_names:
            Document.Data.Tables.Remove(Document.Data.Tables[table.Name])

def main():
    """ main function to filter each table where alarm criteria is 1 and then push to a consolidated single table"""

    alarm_def_dt_name='Ready For Alarming'
    alarm_def_dt, alarm_def_cursor = get_spotfire_dt(alarm_def_dt_name, 'None')
    alarm_list = [alarm_def_cursor['AlarmName'].CurrentValue for selectedmeasure in alarm_def_dt.GetRows(Array[DataValueCursor](alarm_def_cursor.values()))]
    alarm_data = []
    logger.info("Creating data table for ENM export...")

    for alarm in alarm_list:
        try:
            if check_alarm_or_event(alarm) == "Event":
                table_rows = get_table_rows(alarm)
            else:
                table_rows = get_table_rows_alarm(alarm)
            alarm_data.extend(table_rows)
        except Exception as e:
            logger.error("No alarm data table found for alarm: " + alarm)
            logger.error(e.message)

    try:
        if alarm_data:    
            create_data_table('Data Table', alarm_data, ALARM_COLUMNS)
            logger.info("Created data table for ENM export.")

            tables_to_keep = ["Alarm Definitions","empty_data_table",'Data Table', "vwEniqEnm", "Successful_Alarms", "Failed_Alarms"]
            clear_tables(tables_to_keep)

            Document.Properties["RunExportToENMAndENIQ"] = DateTime.UtcNow
        else:
            tables_to_keep = ["Alarm Definitions","empty_data_table", "vwEniqEnm", "Successful_Alarms", "Failed_Alarms"]          
            clear_tables(tables_to_keep)
            logger.info("No alarms met alarm criteria. Nothing to send. Closing file.")
    except Exception as e:
        logger.error("Failed to create ENM data table.")

main()
