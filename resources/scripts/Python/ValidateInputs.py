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
# Name    : ValidateInputs.py
# Date    : 22/02/2021
# Revision: 1.0
# Purpose : Adjusts lookback/data range dropdowns depending on aggregation dropdown
#
# Usage   : PM Alarming
import time
import datetime
from datetime import datetime, timedelta
from System import Array, Object
from System.Collections.Generic import Dictionary, List
from itertools import combinations,tee
import re
import collections
from collections import OrderedDict
import clr
clr.AddReference('System.Data')
from System.Data.Odbc import OdbcConnection,OdbcType
# Spotifre imports
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Data import *
from System import DateTime

# Denotes the index of a char in a string.
CharIdx = collections.namedtuple('CharIdx', 'index char')

# Indicates the index of the opening parenthesis and the associated function (if any). 
OpenParenthesisInfo = collections.namedtuple('OpenParenthesisInfo', 'function_name index')

# Indicates the indexes of the matching opening and closing parenthesis and the associated function name (if any).
MatchParenthesis = collections.namedtuple('MatchParenthesis', 'function_name open_par_idx close_par_idx')

# Represents a variable name.
var_re = r'[a-zA-Z_]+[0-9a-zA-Z_.]*'

time_aggregation = Document.Properties["Aggregation"]

class AlarmState:
    Active = 'Active'
    Inactive = 'Inactive'

class AlarmColumn:
    AlarmName = 'AlarmName'
    AlarmType = 'AlarmType'
    MeasureName = 'SelectedMeasureList'
    Severity = 'Severity'  
    AlarmState = 'AlarmState'
    TableTypePlaceHolder = 'TableType'
    NECollection = 'NECollection'  
    SpecificProblem = 'SpecificProblem'
    ProbableCause = 'ProbableCause'
    Schedule = 'Schedule'
    Aggregation = 'Aggregation'
    MeasureType = 'MeasureType'
    SingleOrCollection = 'SingleOrCollection'
    SingleNodeValue = 'SingleNodeValue'
    NodeType = 'NodeType'
    LookBackVal = 'LookBackVal'
    LookBackUnit = 'LookBackUnit'
    DataRangeVal = 'DataRangeVal'
    DataRangeUnit = 'DataRangeUnit'
    PeriodDuration = 'PeriodDuration'
    TableName = 'TableName'
    EniqName = 'EniqName' 


def create_cursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""

    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))
    return cusrDict


def get_alarm_definitions_names(data_table_name):
    """Get Alarm Rule names stored in Alarm Definitions table

    Arguments:
        data_table_name {string} -- Alarm Definitions data table name

    Returns:
        alarm_definitions {list} -- list of Alarm Rules names

    """
    alarm_definitions = []
    if Document.Data.Tables.Contains(data_table_name):
        data_table = Document.Data.Tables[data_table_name]
        rows = IndexSet(data_table.RowCount, True)
        alarm_name = Document.Properties["AlarmName"]
        cursor = DataValueCursor.CreateFormatted(data_table.Columns["AlarmName"])
        for row in data_table.GetRows(rows, cursor):
            alarm_definitions.append(cursor.CurrentValue)
    return alarm_definitions


def schedule_interval_error(schedule_interval, aggregation_period):
    """Checks if the schedule interval and aggregation period are a valid combo
    i.e. the aggregation period cannot be less than the schedule interval

    Arguments:
        schedule_interval -- string
        aggregation_period -- string

    Returns:
        interval_error -- boolean
    """
    agg_per = {
    "None": 0,
    "1 Hour": 60,
    "1 Day": 1440 
    }
    interval_error = True
    if aggregation_period in agg_per:
       if agg_per[aggregation_period] > int(schedule_interval) :
           interval_error = False

    return interval_error


def get_table_name(measure_name, measure_mapping_data_table, node_type):
    """gets a measure's table name with a given measure name

    Arguments:
        measure_name -- string measure
        measure_mapping_data_table -- Measure Mapping data table object

    Returns:
        selected measure's table name
    """
    measure_filter = measure_mapping_data_table.Select("[Measure]= '" + measure_name + "' AND [Node Type]= '"+node_type+"'")
    if not measure_filter.AsIndexSet().IsEmpty:
        table_name_cursor = DataValueCursor.Create(measure_mapping_data_table.Columns['TABLENAME'])
        for row in measure_mapping_data_table.GetRows(measure_filter.AsIndexSet(),table_name_cursor):
            return table_name_cursor.CurrentValue


def get_measure_type(measure, measure_mapping_data_table, node_type):
    """gets a measure's type with a given measure name

    Arguments:
        measure_name -- string measure
        measure_mapping_data_table -- Measure Mapping data table object

    Returns:
        selected measure's measure type
    """
    measure_table_filter = measure_mapping_data_table.Select("[Measure]= '" + measure + "' AND [Node Type]= '"+node_type+"'")
                
    if not measure_table_filter.AsIndexSet().IsEmpty:
        measure_type_cursor = DataValueCursor.Create(measure_mapping_data_table.Columns['Measure Type'])
        for row in measure_mapping_data_table.GetRows(measure_table_filter.AsIndexSet(),measure_type_cursor):
            return measure_type_cursor.CurrentValue
        

def check_measures_from_single_table(measures_list, measure_mapping_data_table, node_type):
    """Checks if all selected measures are from the same table

    Arguments:
        measure_list {list} -- selected measures list
        measure_mapping_data_table {data table} -- Measure Mapping data table object

    Returns:
        is_same_table {boolean} 
    """   
    table_list = []
    pre_table_list = []
    measur_type = []
    temp_list_measure = []
    for measure in measures_list:
        pre_table_list.append(get_table_name(measure, measure_mapping_data_table, node_type))
        measur_type.append(get_measure_type(measure, measure_mapping_data_table, node_type))
        
    for i in range(0,len(pre_table_list)):
        if measur_type[i] == 'Counter':
            temp_list_measure.append(pre_table_list[i])

    table_list = ([tabl for tabl in pre_table_list if tabl not in temp_list_measure])

    if len(table_list) <= 0 : 
        is_same_table = True
    is_same_table = all(table_name == table_list[0] for table_name in table_list)
    
    if is_same_table == True and len(table_list) > 0 and len(temp_list_measure) > 0:
        TablesPresent = table_list[0].split(',')
        for tables in temp_list_measure:
            if tables in TablesPresent:
                is_same_table = True
            else:
                is_same_table = False
    elif is_same_table == True and len(table_list) <= 0 and len(temp_list_measure) > 0:
        table_list = temp_list_measure 
        is_same_table = all(table_name == table_list[0] for table_name in table_list)
    Document.Properties['TableName'] = table_list[0]
      
    return is_same_table


def validate_empty_fields(od):
    """
    checking for values if it contains any symbols which is not accepted by ENM

    Arguments:
        od {ordered list} -- Alarm rule manager, rule creation fields

    Returns:
        response {string} -- Emptry string or error message

    """
    response = ''
    if len(od) != 0:
        for key, value in od.items():
            if key == "Alarm Type" and value == "Threshold":
                response = key
                break
            if value != None:
                if not value.strip():
                    response = key
                    break
            else:
                response = "Required Field cannot be None"
    return response

def length_check(arg):
    """checking the length of SpecificProblem field."""
    response = ''
    if len(arg.strip()) > 100:
        response = "Specific Problem cannot be more than 100 characters long."
        
    return response

def is_valid_for_ENM(*args):
    """checking for values that are unacceptable characters in ENM 
    **kwargs --> takes inputs a key value pair:
    """
    response = True
    if len(args) != 0:
        for value in args:
            if '#' in value or '?' in value:
                response = False
    return response


def match_parenthesis(indexes, result=None):
    """Returns a list of tuples with all the matching open and close parenthesis indexes."""
    result = result or []
    if len(indexes) % 2 != 0:
        raise ValueError('The parenthesis are not balanced.')
    indexes = [e for e in indexes]
    if len(indexes) == 0:
        return result
    indexes_cp = [e for e in indexes]
    for o, c in zip(indexes_cp, indexes_cp[1:]):
        if o[1] == '(' and c[1] == ')':
            result.append((o[0], c[0]))
            indexes.remove(o)
            indexes.remove(c)
    return match_parenthesis(indexes, result)


def list_open_parenthesis_info(expr):
    """Returns a list of OpenParenthesisInfo for the specified expression."""
    return [OpenParenthesisInfo(m.group(1) or None, m.end(0)-1)  for m in re.finditer(r'([a-zA-Z0-9_]*) *\(', expr)]


def index_parenthesis(expr):
    """Returns a list of CharIdx for the specified expression."""
    return [CharIdx(idx, char) for idx, char in enumerate(expr) if char in [')', '(']]


def get_parenthesis_idx(expr):
    """Returns a list of MatchParenthesis for the given expression."""
    inds = index_parenthesis(expr)
    pars = sorted(match_parenthesis(inds), key=lambda x: x[0])
    functions = (e.function_name for e in list_open_parenthesis_info(expr) if e)
    return [MatchParenthesis(func, idxs[0], idxs[1]) for func, idxs in zip(functions, pars)]


def get_functions(expr):
    """Returns a set with all the function names in the expression."""
    return set(e.function_name for e in get_parenthesis_idx(expr) if e.function_name)


def get_variables(expr):
    """Returns a set of all variables names used in the expression."""
    function_names = get_functions(expr)
    result = expr
    for f in function_names:
        result = re.sub( r'(' + f + r') *\(', '(', result)
    return set(m.group(1) for m in re.finditer('(' + var_re + ')', result))


def validate_errors():
    """
    Checks user inputs are valid

    Returns:
        isValid -- boolean
    """
    is_valid = True
    error_message = ""
    dp = OrderedDict()
    dp["Alarm Name"] = Document.Properties['AlarmName']
    dp["Alarm Type"] = Document.Properties['AlarmType']
    dp["Measure Input"] = Document.Properties['SelectedMeasureList']
    dp["Measure Type"] = Document.Properties['MeasureType']
    dp["Eniq Name"] = Document.Properties['ENIQDataSourcesDropDown']
    dp["Probable Cause Input"] = Document.Properties['ProbableCause']
    dp["Specific Problem Input"] = Document.Properties['SpecificProblem']
    schedule_interval = Document.Properties['Schedule']
    aggregation_period = Document.Properties['Aggregation']
    measure_tables_data_table = Document.Data.Tables['Measure Mapping']
    measure_list = dp["Measure Input"].split(";")
    multitable_kpi_exceptions_list = Document.Properties['MultiTableKPIsExceptions'].split(',')

    if Document.Properties["SingleOrCollection"] == 'Single Node':
        dp['Single Node'] = Document.Properties["SingleNodeValue"]
        
    if Document.Properties[AlarmColumn.SingleOrCollection] == 'Subnetwork':
        dp["Subnetwork"] = Document.Properties["subnetwork"]
    measure_table_cur = create_cursor(measure_tables_data_table)
    node_type = Document.Properties['NodeType']
    system_area = Document.Properties['SystemArea']
    for measure in measure_list:
        selected_measure = measure_tables_data_table.Select("[Measure]= '" + measure + "'")

    if Document.Properties['NodeType'] == "M-MGw":
        
        hi_lo_flag = False
        
        if time_aggregation == "1 Hour":
            
            measure_mapping_table_name = 'Measure Mapping'
            measure_mapping_data_table = Document.Data.Tables[measure_mapping_table_name]
            measure_table_cursor = create_cursor(measure_mapping_data_table)

            node_type = Document.Properties['NodeType']
            
            hi_lo_list = []
            alarm_measure_list = []
            alarm_measure_list.append(Document.Properties['SelectedKPI1'])
            alarm_measure_list.append(Document.Properties['SelectedKPI2'])
            alarm_measure_list.append(Document.Properties['SelectedKPI3'])
            alarm_measure_list.append(Document.Properties['SelectedKPI4'])
            complete_alarm_measure = ";".join(alarm_measure_list)
            alarm_measure = ";".join(alarm_measure_list)
            alarm_measures_list = alarm_measure.split(";")

            for alarm_measure in alarm_measures_list:
           
                measure_table_filter = measure_mapping_data_table.Select("[Measure]= '" + alarm_measure + "' AND [Node Type]= '"+node_type+"'")
                
                for measure in measure_mapping_data_table.GetRows(measure_table_filter.AsIndexSet(), Array[DataValueCursor](measure_table_cursor.values())):

                     raw_formula = measure_table_cursor['Formula'].CurrentValue.upper()
                     measure = measure_table_cursor['Measure'].CurrentValue.upper()
                     measure_type = measure_table_cursor['Measure Type'].CurrentValue.upper()

                     counters = get_variables(raw_formula)
                     
                     for counter in counters:
                         counter = counter.split('.')[1]
                         if counter[-2:] == "HI" or counter[-2:] == "LO":
                             if measure_type == 'COUNTER':
                                 hi_lo_list.append(counter)
                                 hi_lo_flag = True
                             elif measure_type == 'KPI':
                                  
                                 if measure not in hi_lo_list:
                                     hi_lo_list.append(measure)
                                     hi_lo_flag = True
        if hi_lo_flag == True:
            error_message = "The level of Aggregation is not currently available for the selected KPI(s) or Counter(s). Remove the KPI(s) or Counter(s) below or change the level of Aggregation. KPI(s) or Counter(s) affected: "+(', '.join(map(str, hi_lo_list)))     
            is_valid = False
        else:
            error_message = ""

    if time_aggregation == "1 Hour":

        alarm_measure_list = []
        alarm_measure_list.append(Document.Properties['SelectedKPI1'])
        alarm_measure_list.append(Document.Properties['SelectedKPI2'])
        alarm_measure_list.append(Document.Properties['SelectedKPI3'])
        alarm_measure_list.append(Document.Properties['SelectedKPI4'])
        alarm_measure = ";".join(alarm_measure_list)
        alarm_measures_list = alarm_measure.split(";")

        effected_kpis_list = []
        multitable_effected_kpi_flag = False
        
        for alarm_measure in alarm_measures_list:
            if alarm_measure != str.Empty and alarm_measure in multitable_kpi_exceptions_list:
                multitable_effected_kpi_flag = True
                effected_kpis_list.append(alarm_measure)
        if multitable_effected_kpi_flag == True:
            error_message = "The level of Aggregation is not currently available for the selected KPI(s). To Fetch Data remove the KPI(s) below or change the level of Aggregation. KPI(s) affected: "+(', '.join(map(str, effected_kpis_list)))
            is_valid = False
                
    if dp["Measure Input"] != '':
        for kpi in measure_tables_data_table.GetRows(selected_measure.AsIndexSet(), Array[DataValueCursor](measure_table_cur.values())):
            if Document.Properties['NodeType'] == measure_table_cur['FilteredNodeType'].CurrentValue:
                curr_node_type = measure_table_cur['Node Type'].CurrentValue
                curr_system_area = measure_table_cur['System Area'].CurrentValue
        
                if (curr_node_type != node_type) or (curr_system_area != system_area):
                    error_message  += "Please ensure system area and node type are correct."
                    is_valid = False
    elif Document.Properties[AlarmColumn.SingleOrCollection] == 'Collection':

        dp["NE Collection"] = Document.Properties["NECollection"]
        
    


    if check_measures_from_single_table(measure_list,measure_tables_data_table, node_type) != True:
        error_message = "Selected measures must be present in the same table"
        is_valid = False

    empty_fields = validate_empty_fields(dp)
    ENM_valid = is_valid_for_ENM(dp["Alarm Name"], dp["Probable Cause Input"], dp["Specific Problem Input"])
    specific_len = length_check(dp["Specific Problem Input"])

    if len(empty_fields)>0 :
        is_valid = False
        error_message = " please provide Value for: " + str(empty_fields)
    if ENM_valid == False:
        is_valid = False
        error_message  += ", please remove '#' or '?' from the input fields"
    if len(specific_len)>0 :
        is_valid = False
        error_message = str(specific_len)
     
    Document.Properties['ValidationError'] = error_message

    return is_valid



new_alarm_name = Document.Properties["AlarmName"]
alarm_definitions = get_alarm_definitions_names("Alarm Definitions")

if validate_errors():
    if new_alarm_name in alarm_definitions and Document.Properties["IsEdit"] == "Create":
        Document.Properties["ValidationError"] = 'Error: Alarm Definition "%s" already exists' % new_alarm_name
    else:
        Document.Properties["fetchData"] = DateTime.UtcNow
        #print "End!"