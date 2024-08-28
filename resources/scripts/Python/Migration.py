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
# Name    : Migration.py
# Date    : 27/07/2021
# Revision: 2.0
# Purpose :
#
# Usage   : PM Alarming
#

import time
import clr
clr.AddReference('System.Data')
from System.Data.Odbc import OdbcConnection, OdbcType
from System import Array
import collections
from collections import namedtuple,OrderedDict
from Spotfire.Dxp.Data.Formatters import *
from System.IO import  MemoryStream

import re
from System.Collections.Generic import List
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings

from System import Array, Byte
from System.Text import UTF8Encoding
from Spotfire.Dxp.Framework.ApplicationModel import  ProgressService,NotificationService
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
import ast


eniq_name = "NetAn_ODBC"
CONN_STRING = "DSN=" + eniq_name
ps = Application.GetService[ProgressService]()

kpis = {}
alarm_for_template=[]

PLACEHOLDER_EXPRESSIONS = {
    "LongInteger": "0",
    "DateTime": "DATETIME('01/01/2021')",
    "Integer": "0",
    "String": "'Placeholder'",
    "Real": "0.0",
    "SingleReal": "0.0",
    "Boolean": "True",
    "LongInteger": "0",
    "Time": "Time(1)",
    "TimeSpan": "TimeSpan(1)",
    "(Empty)":"'0'"
}

time_columns = {
    'None':'DATETIME_ID',
    '1 Hour': 'DATETIME_ID',
    '1 Day':'DATE_ID'
}

interval_dict = {
    'None': 'mi',
    '1 Hour': 'mi',
    '1 Day': 'day'
}


lookback_dict = {

'Threshold': {'none': {'present_min':15},
              'hour': {'present_min':60},
              'day': {'present_min':0}}
}


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

try:
    conn_string= Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)


# Denotes the index of a char in a string.
CharIdx = collections.namedtuple('CharIdx', 'index char')

# Indicates the index of the opening parenthesis and the associated function (if any).
OpenParenthesisInfo = collections.namedtuple('OpenParenthesisInfo', 'function_name index')

# Indicates the indexes of the matching opening and closing parenthesis and the associated function name (if any).
MatchParenthesis = collections.namedtuple('MatchParenthesis', 'function_name open_par_idx close_par_idx')

# Represents a variable name.
var_re = r'[a-zA-Z_]+[0-9a-zA-Z_.]*'

# Represents a grouping function.
gfunc_re = r'(AVG|MIN|MAX|SUM) *\((.*?)\)'

# Represents a table name.
table_re = var_re + r'\.'


def strip_table_names(expr):
    """Removes the table names from the expression.
    Ex: 'AVG(table1.column1)' --> 'AVG(column1)'
    """
    return re.sub(table_re, '', expr)


def add_brackets_to_variables(expr):
    """Adds square brackets to the variable names in the expression.
    Ex: 'MAX(a) - 1/(b+3)'   --> 'MAX([a]) - 1/([b]+3)'
    """
    variables = get_variables(expr)
    result = expr
    for v in variables:
        result = re.sub('\b(' + v + ')\b', r'[\1]', result, flags=re.IGNORECASE)
    return result


def remove_group_functions(expr):
    """Removes all the grouping functions from the expression.
    Ex: 'AVG(x) - MAX(y)'   --> '(x) - (y)'
    """
    return re.sub( gfunc_re  , r'\2', expr, flags=re.IGNORECASE)


def index_parenthesis(expr):
    """Returns a list of CharIdx for the specified expression."""
    return [CharIdx(idx, char) for idx, char in enumerate(expr) if char in [')', '(']]


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


def get_parenthesis_idx(expr):
    """Returns a list of MatchParenthesis for the given expression."""
    inds = index_parenthesis(expr)
    pars = sorted(match_parenthesis(inds), key=lambda x: x[0])
    functions = (e.function_name for e in list_open_parenthesis_info(expr) if e)
    return [MatchParenthesis(func, idxs[0], idxs[1]) for func, idxs in zip(functions, pars)]


def substitute(text, idx, char):
    """Subistitute a char in the specified position of the string."""
    chars = list(a for a in text)
    chars[idx] = char
    return ''.join(chars)


def convert_is_null(expr):
    """Converts the SQL function ISNULL to the equivalent in Spotfire expression."""
    func_idx = get_parenthesis_idx(expr)
    result = expr
    for func_name, open_pidx, close_pidx in func_idx:
        if func_name and func_name.upper() == 'ISNULL':
            result = substitute(result, open_pidx, '#')
            result = substitute(result, close_pidx, '#')
            result = re.sub(r'ISNULL *#(.+),(.+)#', r'If(\1 Is Null, \2, \1)', result, flags=re.IGNORECASE)
    return result


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


def spotfire_expression(expr):
    """Convert a SQL expression into a Spotfire expression."""

    result = strip_table_names(expr)
    result = remove_group_functions(result)
    result = add_brackets_to_variables(result)
    return convert_is_null(result)

def create_cursor(table):
    """Create cursors for a given table, these are used to loop through columns"""

    curs_list = []
    col_list = []

    for column in table.Columns:
        if column.Properties.DataType.ToString() in ['DateTime']:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name], DATETIME_FORMATTER))
            col_list.append(table.Columns[column.Name].ToString())
        else:
            curs_list.append(DataValueCursor.CreateFormatted(table.Columns[column.Name]))
            col_list.append(table.Columns[column.Name].ToString())
    cusr_dict = dict(zip(col_list, curs_list))

    return cusr_dict





def get_node_list(ne_collection):
    '''Return a list of nodes in a node collection (or just a single node if no collection)'''
    ne_list = []
    node_collections_data_table_name = 'NodeCollection'
    filtercondition = "[CollectionName] ='" + ne_collection + "'"
    node_collections_data_table,node_collection_cur,node_collection_index =get_spotfire_dt(node_collections_data_table_name,filtercondition)

    for _ in node_collections_data_table.GetRows(node_collection_index, Array[DataValueCursor](node_collection_cur.values())):
        ne_list.append(node_collection_cur['NodeName'].CurrentValue)

    # check if its a single node (i.e. an empty list because node not included in collection list)
    if not ne_list:
        node_list = "'" + ne_collection + "'"
    else:
        node_list = ','.join("'{}'".format(i) for i in ne_list)

    return node_list

def replace_rops(replace_string, agg_level, agg_val):

    if agg_level == 'None' or agg_level == 'raw':
        replace_string = re.sub(r'<ROPS>/(15|((AVG|MAX)\(\w*\.PERIOD_DURATION\)))' ,'1', replace_string, flags=re.IGNORECASE)
    else:
        replace_string  = replace_string.replace('<ROPS>', agg_val)
    return replace_string

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


def get_period_duration(table_names):
    query_list=[]
    period_duration={}
    for table_name in table_names:
        q='SELECT max(PERIOD_DURATION) as "PERIOD_DURATION",'+ '"' + table_name + '"' + ' as "Table"  FROM  ' + table_name
        query_list.append(q)
    query=' Union '.join(query_list)
    period_duration_table_name="period_duration"
    run_query(period_duration_table_name,query,'period_duration_table')
    period_duration_dt, period_duration_cursor= get_spotfire_dt(period_duration_table_name)

    for _ in period_duration_dt.GetRows(Array[DataValueCursor](period_duration_cursor.values())):
        tablename = period_duration_cursor['Table'].CurrentValue
        p_d= period_duration_cursor['PERIOD_DURATION'].CurrentValue
        if  "(Empty)" in p_d:
            period_duration.update({tablename:15})
        else:
            period_duration.update({tablename: p_d})
    Document.Data.Tables.Remove(period_duration_dt)
    return period_duration


def change_table_extension(table_name):
    """Changes PM data table sufix from RAW to DAY

    Arguments:
        table_name {string} -- PM Data table name
    Returns:
        updated_table_name {string} -- PM Data table name with replaced sufix
    """
    temp = table_name.split('_RAW')
    table_name_substring=temp[0]
    updated_table_name=str(table_name_substring)+'_DAY'
    return updated_table_name

def get_spotfire_dt(source_data_table_name, filtercondition='None'):
    """ for a given spotfire table return the tablename, cursor and indexset """

    source_data_table = Document.Data.Tables[source_data_table_name]
    source_cur = create_cursor(source_data_table)
    if filtercondition != 'None':
        index_filter = Document.Data.Tables[source_data_table_name].Select(filtercondition)
        index_set = index_filter.AsIndexSet()
        return source_data_table, source_cur, index_set

    return source_data_table, source_cur

def get_table_name(measure_name):
    """gets a measure's table name with a given measure name

    Arguments:
        measure_name -- string measure
        measure_mapping_data_table -- Measure Mapping data table object

    Returns:
        selected measure's table name
    """
    measure_mapping_table_name = 'Measure Mapping'
    filtercondition = "[Measure] = '" + measure_name + "'"
    measure_mapping_dt, measure_mapping_cursor, measure_mapping_index = get_spotfire_dt(measure_mapping_table_name,filtercondition)
    tablename=""
    if not measure_mapping_index.IsEmpty:
        for _ in measure_mapping_dt.GetRows(measure_mapping_index,Array[DataValueCursor](measure_mapping_cursor.values())):
            tablename = measure_mapping_cursor['TABLENAME'].CurrentValue

        return tablename
    else:
        return None


def get_time_limits(alarm_info):
    ''' get the time limits for the date clause fucntion based on lookback, schedule, data range and type of alarm'''
    time_limits = {}

    alarm_type = alarm_info["alarm_type"]
    time_val = alarm_info["time_val"]
    period_duration = alarm_info['period_duration']
    past_min='0'
    past_max=0
    present_min = lookback_dict[alarm_type][time_val]['present_min']

    if alarm_type == 'threshold' :
        if period_duration == '5' and time_val == 'none':
            present_min = lookback_dict[alarm_type][time_val]['present_min']/3

    time_limits['present'] = {
        'min': str(present_min),
        'max': '0'
                }
    time_limits['past'] = {
        'min': str(past_min),
        'max': str(past_max)
    }

    return time_limits


def get_time_clause(alarm_info, time_limits, time_limit_check, table):
    ''' based on alarm type, will have different time frames for the date clause '''
    sql_parameter_values = {
        'table_and_time_field': table + "." + time_columns[alarm_info["aggregation"]],
        'interval': interval_dict[alarm_info["aggregation"]],
        'min_time_limit': time_limits[time_limit_check]['min'],
        'max_time_limit': time_limits[time_limit_check]['max']
        }
    operator_min = ">="
    operator_max = "<"

    sql_parameter_values['operator_min'] = operator_min
    sql_parameter_values['operator_max'] = operator_max

    # the max date time can be 0, which means that it will effectively be the same as saying = @datetime
    date_time_sql = """({table_and_time_field} {operator_min} (DATEADD({interval},-{min_time_limit}, @dateTime)))
        AND ({table_and_time_field} {operator_max} (DATEADD({interval},-{max_time_limit}, @dateTime)))""".format(**sql_parameter_values)

    # for day alarms, for theshold the date id must equal the last loaded date (min_time_limit = 0).
    # any other time, we just go back x amount of time
    if alarm_info["aggregation"] == '1 Day' and time_limit_check == 'present':

        if alarm_info["alarm_type"] in ['threshold','cdt']:
            operator = "="
        else:
            operator = ">"
        sql_parameter_values['operator'] = operator
        date_time_sql = """({table_and_time_field} {operator} (DATEADD({interval},-{min_time_limit}, @dateTime)))""".format(**sql_parameter_values)

    return date_time_sql


def update_keys(keys, aggregation, table_name):
    """Updates the keys columns used in formula, depending on which aggregation is selected

    Arguments:
        aggregation -- aggregation used in SQL query
        keys -- list of keys used in SQL query
    Returns:
        updated_keys --  String list of updated keys
    """
    updated_keys = keys

    if "DC_TIMEZONE" not in updated_keys:
        updated_keys.add("""{0}.DC_TIMEZONE""".format(table_name))

    if aggregation == "1 Hour":
        updated_keys.add("""{0}.HOUR_ID""".format(table_name))
        return updated_keys
    else:
        return updated_keys


def aggregation_time_column(aggregation):
    """Updates the DATE/DATETIME_ID used in formula, depending on which aggregation is selected

    Arguments:
        aggregation -- aggregation used in SQL query
    Returns:
        date_column --  String of either DATE_ID or DATETIME_ID
    """

    date_column = ""
    if aggregation == "None":
        date_column = "DATETIME_ID"
    else:
        date_column = "DATE_ID"

    return date_column


def get_aggregations(counters):
    """Gets a dict of counters used in query with the correct aggregation

    Arguments:
        counters -- list of counters used in SQL query

    Returns:
        counter_aggregations -- dictionary whewre key=counters, value=aggregation
    """

    counter_mapping_table_name = 'Counter Mapping'
    counter_mapping_data_table = Document.Data.Tables[counter_mapping_table_name]
    counter_table_cursor = create_cursor(counter_mapping_data_table)
    counter_aggregations = {}

    for counter in counters:
        table_name = counter.split(".")[0]
        counter = counter.split(".")[1]
        counter_table_filter = counter_mapping_data_table.Select("[COUNTER]= '" + counter + "'")

        for _ in counter_mapping_data_table.GetRows(counter_table_filter.AsIndexSet(), Array[DataValueCursor](counter_table_cursor.values())):
            counter_name = counter_table_cursor['COUNTER'].CurrentValue
            aggregation = counter_table_cursor['TIMEAGGREGATION'].CurrentValue

            if counter_name in counter:
                counter_aggregations[table_name+"."+counter_name] = aggregation

    return counter_aggregations

def aggregate_counters(counters, aggregation, table_type):
    """Adds an aggregation to the counters e.g SUM(PM_CELL_DOWNTIME_MAN)

    Arguments:
        counters -- list of aggregation used in SQL query
        aggregation -- aggregation used in SQL query
    Returns:
        counters --  updated list of counters with aggregations attached
    """
    if aggregation != '1 Hour':
        return sorted(list(counters), key=lambda x: (('EUTRANCELLFDD' in x or 'EUTRANCELLTDD' in x), x))
    else:
        counters_aggregations = get_aggregations(counters)
        updated_counters = set()

        for counter, agg in counters_aggregations.items():
            updated_counters.add(agg+'('+counter+') as "'+counter.split(".")[1]+'"')

        return sorted(list(updated_counters), key=lambda x: (('EUTRANCELLFDD' in x or 'EUTRANCELLTDD' in x), x))


def check_if_kpi_specific_where_clause(where_clause):
    """add special where clause like FLEX_FILTER=2 etc."""
    if where_clause != '(Empty)':
        return 'AND ' + where_clause
    return ''

def get_de_duplicated_keys(full_key_list):
    """for every key in the key list, if there are duplicate keys remove them"""
    de_duped_key_list = []
    duplicate_keys = set()
    for key in full_key_list:
        table_name = key.split(',')[0]
        table_key = table_name.split('.')[1]

        if table_key not in duplicate_keys:
            de_duped_key_list.append(key)
            duplicate_keys.add(table_key)

    return de_duped_key_list

def get_alarm_query_details_complex_kpi(alarm_info,time_limits, time_flag, time_flag_value):
    """returns a list of dictionaries where each dictionary contains the details for a sub-query involved in a complex UNION query"""
    details_per_query_list = []
    keys_to_avoid = Document.Properties['UnionKeys'] #these columns are avoided because UNION of sub-queries does not work if the column names don't match
    for formula_part in alarm_info["formula"].split('UNION'):
        table_set = set()
        key_set = set()
        element_set = set()
        counter_set = set()
        joins_per_query = []
        
        counter_set_formula = get_variables(formula_part)
        for counter in counter_set_formula:
            table_set.add(counter.split(".")[0])
        for table in table_set:
            full_key_list = update_keys(alarm_info["keys"], alarm_info["aggregation"],table)
        for counter in alarm_info["counters"]:
            if counter.split(".")[0] in table_set:
                counter_set.add(counter)

        for key in set(full_key_list):
            if key.split(".")[0] in table_set and (key.split(".")[1]).upper() not in keys_to_avoid.split(","):
                key_set.add(key)

        for element in alarm_info["element"].split(','):
            if element.split(".")[0] in table_set:
                element_set.add(element)
        joins_list = get_common_joins(list(element_set),list(table_set),time_columns[Document.Properties['Aggregation']])
        if alarm_info["join_keys"]:
            for map_col in alarm_info["join_keys"]:
                map_col_keys = map_col.split("=")
                if (map_col_keys[0].split("."))[0] in table_set and (map_col_keys[1].split("."))[0] in table_set:
                    joins_per_query.append(map_col)
        if joins_list:
            join_keys_list = joins_per_query + joins_list
        else:
            join_keys_list = joins_per_query
        join_keys = ""
        for join in join_keys_list:
            join_keys = join_keys + " AND " + join

        counters = check_for_duplicate_counters(counter_set,alarm_info['group_aggregation'],alarm_info["aggregation"],alarm_info["table_type"],alarm_info["table_names"])
        group_by = get_group_by_clause(key_set,list(table_set)[0], alarm_info["aggregation"])
        key_list = sorted(list(key_set), key=lambda x: (('EUTRANCELLFDD' in x or 'EUTRANCELLTDD' in x), x))
             
    
        details_per_query_dict = {
		    'date_time_clause': get_time_clause(alarm_info, time_limits, time_flag,(list(table_set))[0]),
            'keys': (' ,'.join(key_list)), 
            'from_table': (' ,'.join(table_set)),
            'element_table': list(table_set)[0],
            'time_column': aggregation_time_column(alarm_info["aggregation"]), 
            'counters': counters,
            'element_field': list(element_set)[0],
            'elements': list(element_set)[0],
            'ne_list': get_node_list(alarm_info["ne_collection"]),
            'group_by': group_by,
            'past_present': time_flag_value,
            'kpi_specific_where_clause': check_if_kpi_specific_where_clause(alarm_info["kpi_specific_where_clause"]),
            'join_keys': join_keys 
        }
        details_per_query_list.append(details_per_query_dict)
    		
    return details_per_query_list

def checkIfDuplicates(counters):
    ''' Check if given list contains any duplicates '''
    if len(counters) == len(set(counters)):
        return False
    else:
        return True


def aggregate_counters_for_duplicates(counters,aggregate_function,table_names,aggregation):
    '''Add the aggregation function to the counters '''
    counter_set = set()
    if '_EUTRANCELLFDD_' in table_names or '_EUTRANCELLTDD_' in table_names:
        counter_set=counters
        return counter_set
    if aggregation != '1 Hour':
        counter_set = set(counter+" as "+counter.replace(".", "_") for counter in counters)
    else:
        counter_set = set(agg_fun.split(".")[0]+"("+counter+") as "+counter.replace(".", "_") for counter in counters for agg_fun in aggregate_function if counter.split(".")[1] == agg_fun.split(".")[1])   
    return counter_set

def check_for_duplicate_counters(counters,group_agg,aggregation,table_type,table_names):
    counters_to_return = ''

    if checkIfDuplicates([c.split(".")[1] for c in counters]): 
        counters_to_return = aggregate_counters_for_duplicates(counters,group_agg,table_names,aggregation)
        counters_to_return = ", ".join(counters_to_return)
    else:
        counters_to_return = ", ".join(aggregate_counters(counters, aggregation, table_type))

    return counters_to_return

def get_alarm_query_details(alarm_info,time_limits, time_flag, time_flag_value):
    """ build a dictionary to get the variables for the alarm query """

    multi_table_check = alarm_info["table_names"].split(',')
    if 'UNION' in alarm_info["formula"]:
        details_per_query_list =  get_alarm_query_details_complex_kpi(alarm_info,time_limits, time_flag, time_flag_value)
    else:
        full_key_list = update_keys(alarm_info["keys"], alarm_info["aggregation"],alarm_info["eniqtablename"])	
        join_keys = ""
        for join in alarm_info["join_keys"]:
            join_keys = join_keys + " AND " + join
        kpiformula = alarm_info['formula']
        counters = check_for_duplicate_counters(alarm_info['counters'],alarm_info['group_aggregation'],alarm_info["aggregation"],alarm_info["table_type"],alarm_info["table_names"])

        counters_set = set(counters.split(','))
        
        group_by = get_group_by_clause(alarm_info["keys"],alarm_info["eniqtablename"], alarm_info["aggregation"])
        keys = ', '.join(full_key_list)
        elements = "".join(alarm_info["element"].split(',')[0])
        element_field = alarm_info["element"].split(',')[0]
        details_per_query_list = [{
            'date_time_clause': get_time_clause(alarm_info, time_limits, time_flag,alarm_info["eniqtablename"]),
            'keys': keys, 
            'from_table': alarm_info["table_names"],
            'element_table': alarm_info["eniqtablename"],
            'time_column': aggregation_time_column(alarm_info["aggregation"]), 
            'counters': counters,
            'element_field': element_field,
            'elements': elements,
            'ne_list': get_node_list(alarm_info["ne_collection"]),
            'group_by': group_by,
            'past_present': time_flag_value,
            'kpi_specific_where_clause': check_if_kpi_specific_where_clause(alarm_info["kpi_specific_where_clause"]),
            'join_keys': join_keys
        }]
    return details_per_query_list



def build_single_table_query(alarm_query_details_list):
    """ build query for single table query -- for counters/kpi"""
    for alarm_query_details_dict in alarm_query_details_list:
        sql_query = """SELECT {keys},{element_field} AS 'ELEMENT',{table}.{time_column}, {counters} {past_present}
                FROM {table}
                WHERE {date_time_clause}
                AND {table}.ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED')
                AND {elements} IN ({ne_list})
                {group_by}
                """.format(**alarm_query_details_dict)

    return sql_query

def build_table_query(alarm_query_details_list):
    """ build query for single/multi table query -- for counters/kpi"""
    '''assumption(for complex KPIs) - UNION is performed between 2 queries - needs to be updated further for more than 2 queries (no example so far)'''
    sql_query_list = []
    for alarm_query_details_dict in alarm_query_details_list:
        sql_query = """SELECT {keys},{element_field} AS 'ELEMENT',{element_table}.{time_column}, {counters} {past_present}
                FROM {from_table} 
                WHERE {date_time_clause} 
                AND {element_table}.ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED')
                AND {elements} IN ({ne_list})
                {kpi_specific_where_clause}
                {join_keys}
                {group_by}
                """.format(**alarm_query_details_dict)
        sql_query_list.append(sql_query)
    if len(sql_query_list) == 2: 
        updated_sql_query = sql_query_list[0] + " UNION " + sql_query_list[1]
    else:
        updated_sql_query = sql_query_list[0]
    return updated_sql_query

def build_single_table_query_temp(alarm_query_details_list):
    """ build query for single table query -- for counters/kpi"""

    sql_query_list = []
    for alarm_query_details_dict in alarm_query_details_list:
        sql_query = """SELECT {keys},{element_field} AS 'ELEMENT',{element_table}.{time_column}, {counters} {past_present}
                FROM {from_table}
                WHERE 1=0
                {group_by}
                """.format(**alarm_query_details_dict)
        sql_query_list.append(sql_query)
    if len(sql_query_list) == 2: 
        updated_sql_query = sql_query_list[0] + " UNION " + sql_query_list[1]
    else:
        updated_sql_query = sql_query_list[0]
    return updated_sql_query

def get_group_by_clause(keys, table, aggregation):
    """Gets the group by clause used in the SQL, returns empty if no aggregation used
    
    Arguments:
        keys -- list of keys used in SQL query
        table -- table used in SQL query
        aggregation -- aggregation used in SQL query
    Returns:
        group_by --  group by String
    """
    group_by = ""
    if aggregation != "1 Hour":
        group_by = ""
    else:
        group_by = """GROUP BY {keys}, {table}.HOUR_ID, {table}.DATE_ID""".format(table=table, keys =','.join(keys))
    return group_by

def replace_node_collection(alarm_query_details_list):
    ''' the worker files need to recreate the node collection if it ever changes, so assign collection name and replace later'''
    for alarm_query_details_dict in alarm_query_details_list:
        alarm_query_details_dict["ne_list"] = '@node_collection'

    return alarm_query_details_list


def get_all_table_names():
    filtercondition="[ThresholdValue] is not Null OR [MeasureName] ~= 'EUTRANCELLFDD' OR [MeasureName] ~= 'EUTRANCELLTDD'"
    alarm_def_dt, alarm_def_cursor, alarm_def_index=get_spotfire_dt('Alarm Definitions',filtercondition)
    measure_names=[]
    for _ in alarm_def_dt.GetRows(alarm_def_index,Array[DataValueCursor](alarm_def_cursor.values())):
        measure_names.append(update_kpi_name(alarm_def_cursor['MeasureName'].CurrentValue))
    table_names=map(get_table_name,measure_names)
    return table_names
	
def update_kpi_name(kpi):
    return (((kpi.replace("EUTRANCELLFDD","").replace("EUTRANCELLTDD","")).rstrip()).rstrip("-")).rstrip()



def get_alarm_details():
    '''
       Creates and returns a dictionary with all details of the active alarm(s) in Alarm Definitions table
    '''
    alarm_rules = {}
    table_names=get_all_table_names()
    if not table_names:
        return ""
    single_table_names=[]
    table_names = [name for name in table_names if name != None]
    for t in table_names:
        
        single_t = t.split(",")[0]
        if single_t not in single_table_names:
            single_table_names.append(single_t)
    period_duration_all=get_period_duration(single_table_names)
 
    table_type = 'RAW'

    alarm_def_dt_name = 'Alarm Definitions'
    look_back_val = 1
    data_range_val = 1
    alarm_type = 'Threshold'
    filtercondition="[ThresholdValue] is not Null OR [MeasureName] ~= 'EUTRANCELLFDD' OR [MeasureName] ~= 'EUTRANCELLTDD'"
    alarm_def_dt, alarm_def_cursor, alarm_def_index = get_spotfire_dt(alarm_def_dt_name,filtercondition)

    for _ in alarm_def_dt.GetRows(alarm_def_index,Array[DataValueCursor](alarm_def_cursor.values())):
        alarm_name = alarm_def_cursor['AlarmName'].CurrentValue
        measure_name = update_kpi_name(alarm_def_cursor['MeasureName'].CurrentValue)
        ne_collection = alarm_def_cursor['NECollection'].CurrentValue
        alarm_def_schedule = alarm_def_cursor['Schedule'].CurrentValue
        condition=alarm_def_cursor['Condition'].CurrentValue
        threshold_value=alarm_def_cursor['ThresholdValue'].CurrentValue
        table_name=get_table_name(measure_name)
        table_name_db=table_name
        if table_name != None: table_name=table_name.split(',')[0]
        period_duration =  period_duration_all.get(table_name)
        aggregation=alarm_def_cursor['Aggregation'].CurrentValue
        if update_kpi_name(measure_name) in Document.Properties['MultiTableKPIsExceptions'].split(',') and aggregation == "1 Hour":
            notify.AddWarningNotification("Exception","Cannot migrate Alarm "+ alarm_name,"Hourly aggregation is not supported for this KPI/RI")
        else:
            if aggregation == '1 Day':
                look_back_unit='DAY'
                data_range_unit='DAY'
                alarm_table_ext='_DAY'
                table_name = change_table_extension(table_name)
                time_val = "day"

            elif aggregation=='1 Hour':
                look_back_unit='HOUR'
                data_range_unit='HOUR'
                alarm_table_ext='_RAW'
                time_val = "hour"
            elif aggregation=='None':
                look_back_unit='ROP'
                data_range_unit='ROP'
                alarm_table_ext='_RAW'
                time_val = "none"



            measure_mapping_table_name = 'Measure Mapping'
            measure_mapping_data_table = Document.Data.Tables[measure_mapping_table_name]
            measure_table_cursor = create_cursor(measure_mapping_data_table)
            alarm_measure_list = []
            alarm_measure_list.append(measure_name)
            complete_alarm_measure = ";".join(alarm_measure_list)
            alarm_measure = ";".join(alarm_measure_list)
            alarm_measures_list = alarm_measure.split(";")
        

            kpi_order = 0

            for alarm_measure in alarm_measures_list:

                measure_table_filter = measure_mapping_data_table.Select("[Measure]= '" + alarm_measure + "'")

                for _ in measure_mapping_data_table.GetRows(measure_table_filter.AsIndexSet(), Array[DataValueCursor](measure_table_cursor.values())):

                    measure_name = measure_table_cursor['Measure'].CurrentValue
                    element = measure_table_cursor['ELEMENT'].CurrentValue
                    measure_type = measure_table_cursor['Measure Type'].CurrentValue
                    counter_type = measure_table_cursor['COLLECTIONMETHOD'].CurrentValue
                    kpi_specific_where_clause = measure_table_cursor['WHERE CLAUSE'].CurrentValue
                    eniq_table_name = measure_table_cursor['TABLENAME'].CurrentValue
                    hardcoded_sql = measure_table_cursor['SQL OPERATOR'].CurrentValue
                    eniq_table_name = eniq_table_name.upper()
                    keys_raw = measure_table_cursor['KEYS'].CurrentValue.split(',')
                    keys_raw = [c.strip() for c in keys_raw]
                    mapping_columns = measure_table_cursor['Mapping Columns'].CurrentValue
                    custom_keys_raw = measure_table_cursor['CUSTOM KEYS'].CurrentValue.split(',')
                    time_aggregations = measure_table_cursor['TIMEAGGREGATION'].CurrentValue.split(",")
                    group_aggregation = measure_table_cursor['GROUPAGGREGATION'].CurrentValue.split(",")

                    custom_keys_raw = [c.strip() for c in custom_keys_raw]

                    if 'ROWSTATUS' in custom_keys_raw:
                        custom_keys_raw.remove('ROWSTATUS')

                    if '(Empty)' not in custom_keys_raw:
                        custom_keys_raw = set([eniq_table_name + '.' + c for c in custom_keys_raw])

                    raw_formula = measure_table_cursor['Formula'].CurrentValue
                    time_col = time_columns[aggregation]
                    custom_keys = set()
                    counters = set()
                    keys = set()

                    if  time_col =='DATE_ID':
                        raw_formula = replace_rops(raw_formula, 'Day', '1440')
                        counters_raw = get_variables(raw_formula)
                        element = element.replace('_RAW', '_DAY')
                        eniq_table_name = eniq_table_name.replace('_RAW', '_DAY')
                        alarm_table_ext = alarm_table_ext.replace('_RAW', '_DAY')
                        mapping_columns = mapping_columns.replace('_RAW', '_DAY')
                        raw_formula = raw_formula.replace('_RAW', '_DAY')

                        for c in counters_raw:
                            c = c.replace('_RAW', '_DAY')
                            counters.add(c)
                        for k in keys_raw:
                            k = k.replace('_RAW', '_DAY')
                            keys.add(k)
                        for ck in custom_keys_raw:
                            ck = ck.replace('_RAW', '_DAY')
                            custom_keys.add(k)
                    else:
                        if aggregation == '1 Hour':
                            raw_formula = replace_rops(raw_formula, 'Hour', '60')
                        else:
                            raw_formula = replace_rops(raw_formula, 'None', '')
                        keys.update(keys_raw)
                        counters = get_variables(raw_formula)
                    if 'UNION' in counters:
                        counters.remove('UNION')
                    kpi_formula = spotfire_expression(raw_formula.split("UNION")[0])
                    elements = element.split(',')
                    keys.update(elements)
                    joins = []
                    table_list = [c.strip() for c in (eniq_table_name.split(','))]
                    if len(table_list)>1 and 'UNION' not in raw_formula:
                        element_list = element.split(',')
                        joins = get_common_joins(element_list,table_list,time_col)
                    if mapping_columns != '(Empty)':
                        mapping_columns = mapping_columns.split(',') 
                        joins = joins + mapping_columns
                    query=""

                    if alarm_name not in alarm_rules:
                        alarm_rules[alarm_name] = {
                            'counters': set(),
                            'counter_type': counter_type,
                            'element': element,
                            'measurename': complete_alarm_measure,
                            'keys': keys,
                            'measure_type': measure_type,
                            'time_val': time_val,
                            'alarm_def_schedule': alarm_def_schedule,
                            'eniqtablename': eniq_table_name.split(",")[0],
                            'alarm_table_ext': alarm_table_ext,
                            'alarm_name': alarm_name,
                            'alarm_type': alarm_type,
                            'ne_collection': ne_collection,
                            'aggregation': aggregation,
                            'look_back_val': look_back_val,
                            'look_back_unit': look_back_unit,
                            'data_range_val': data_range_val,
                            'data_range_unit': data_range_unit,
                            'period_duration': period_duration,
                            'kpi_specific_where_clause': kpi_specific_where_clause,
                            'table_names' : eniq_table_name,
                            'join_keys': joins,
                            'hardcoded_sql':hardcoded_sql,
                            'condition': condition,
                            'threshold_value': threshold_value,
                            'query':query,
                            'table_name_db':table_name_db,
                            'formula':raw_formula,
                            'table_type':table_type,
                            'time_aggregation':set(time_aggregations),
                            'group_aggregation':set(group_aggregation),
                            'rawformula':set()
                        }
                    kpi_order += 1
                    if alarm_name not in kpis:
                        kpis[alarm_name] = {measure_name:{'kpiformula': kpi_formula.split('UNION')[0], 'kpi_order':kpi_order}}
                    else:
                        kpis[alarm_name][measure_name] = {'kpiformula': kpi_formula.split('UNION')[0], 'kpi_order':kpi_order}

                    alarm_rules[alarm_name]['counters'].update(counters)
                    alarm_rules[alarm_name]['keys'].update(keys)
                    alarm_rules[alarm_name]['rawformula'].add(raw_formula)

                    if 'PERIOD_DURATION' in raw_formula:
                        eniq_table_name_list = eniq_table_name.split(",")
                        for name in eniq_table_name_list:
                            alarm_rules[alarm_name]['keys'].add(name+'.PERIOD_DURATION')

    return alarm_rules
	
def get_common_joins(element_list,table_list,time_col):
    """returns a list of common joins to be added in multi-table queries"""
    common_joins = []
    for i in range(len(element_list)-1):
        common_joins.append(element_list[i] + "=" + element_list[i+1])
    for j in range(len(table_list)-1):
        common_joins.append(table_list[j] + ".OSS_ID = " + table_list[j+1] + ".OSS_ID")
        common_joins.append(table_list[j] + "." + time_col + " = " + table_list[j+1] + "." + time_col)
	return common_joins

def build_query(alarm_name, alarm_info):
    ''' for a single alarm - query builder main function - calls other functions to create single table kpi/counters query'''

    #check logload status to get date times
    time_limits = get_time_limits(alarm_info)
    time_flag_value = ",'Present' as TIME_FLAG"
    time_flag = 'present'
    alarm_query_details = get_alarm_query_details(alarm_info, time_limits, time_flag, time_flag_value)
    alarm_query_for_template = build_single_table_query_temp(alarm_query_details)
    db_alarm_query_details = replace_node_collection(alarm_query_details)
    alarm_query_for_scheduler = build_table_query(db_alarm_query_details)
    alarm_details[alarm_name]["query"]=alarm_query_for_scheduler.replace('\n','')
    alarm_for_template.append((alarm_name,alarm_query_for_template.replace('\n','')))
    return alarm_name


def queries_from_alarm_list(alarms):
    ''' Builds a list of tuples
        1st element is TableName and 2nd elemet is sql query
    '''
    return [build_query(alarm_name, alarm_info) for alarm_name, alarm_info in alarms.items()]



@timeit
def run_query(table_name,query,alarm_name):
    try:
        db_settings = DatabaseDataSourceSettings("System.Data.Odbc", CONN_STRING, query)
        ds = DatabaseDataSource(db_settings)

        if Document.Data.Tables.Contains(table_name):

            dataTable = Document.Data.Tables[table_name]
            dataTable.ReplaceData(ds)
        else:
            Document.Data.Tables.Add(table_name, ds)
        return True
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in migrating Alarm "+ alarm_name ,str(e))
        print("Exception: ",e)
        return False

def add_datetime_calc_column(pm_table_name):
    """Adds a calculated column of DATETIME_ID to the pm table created from the SQL query

    Arguments:
        pm_table_name -- name of table to add calculated column to
    Returns:
        none
    """


    column_expression = '''
		DateTime(case
			when [HOUR_ID]=0 then String(Date([DATE_ID]) & " 00:00:00")
			when [HOUR_ID]=1 then String(Date([DATE_ID]) & " 01:00:00")
			when [HOUR_ID]=2 then String(Date([DATE_ID]) & " 02:00:00")
			when [HOUR_ID]=3 then String(Date([DATE_ID]) & " 03:00:00")
			when [HOUR_ID]=4 then String(Date([DATE_ID]) & " 04:00:00")
			when [HOUR_ID]=5 then String(Date([DATE_ID]) & " 05:00:00")
			when [HOUR_ID]=6 then String(Date([DATE_ID]) & " 06:00:00")
			when [HOUR_ID]=7 then String(Date([DATE_ID]) & " 07:00:00")
			when [HOUR_ID]=8 then String(Date([DATE_ID]) & " 08:00:00")
			when [HOUR_ID]=9 then String(Date([DATE_ID]) & " 09:00:00")
			when [HOUR_ID]=10 then String(Date([DATE_ID]) & " 10:00:00")
			when [HOUR_ID]=11 then String(Date([DATE_ID]) & " 11:00:00")
			when [HOUR_ID]=12 then String(Date([DATE_ID]) & " 12:00:00")
			when [HOUR_ID]=13 then String(Date([DATE_ID]) & " 13:00:00")
			when [HOUR_ID]=14 then String(Date([DATE_ID]) & " 14:00:00")
			when [HOUR_ID]=15 then String(Date([DATE_ID]) & " 15:00:00")
			when [HOUR_ID]=16 then String(Date([DATE_ID]) & " 16:00:00")
			when [HOUR_ID]=17 then String(Date([DATE_ID]) & " 17:00:00")
			when [HOUR_ID]=18 then String(Date([DATE_ID]) & " 18:00:00")
			when [HOUR_ID]=19 then String(Date([DATE_ID]) & " 19:00:00")
			when [HOUR_ID]=20 then String(Date([DATE_ID]) & " 20:00:00")
			when [HOUR_ID]=21 then String(Date([DATE_ID]) & " 21:00:00")
			when [HOUR_ID]=22 then String(Date([DATE_ID]) & " 22:00:00")
			when [HOUR_ID]=23 then String(Date([DATE_ID]) & " 23:00:00")
		end)
    '''

    col_names = Document.Data.Tables[pm_table_name].Columns

    if not any(col.Name == "DATETIME_ID" for col in col_names):
        col_names.AddCalculatedColumn("DATETIME_ID", column_expression)
    else:
        Document.Data.Tables[pm_table_name].Columns["DATETIME_ID"].As[CalculatedColumn]().Expression = column_expression


def add_kpi_calc_column(pm_table_name,kpiname,kpiformula):

    kpiname = kpiname.replace(']', ')').replace('[', '(')
    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]

    if kpiname not in pm_column_names:
        pm_columns.AddCalculatedColumn(kpiname.strip(), "Real("+kpiformula+")")


def add_measure_value_column(pm_table_name,measure_count,measurename):

    measurename = measurename.replace(']', ')').replace('[', '(')
    current_threshold_column_name="MEASUREVALUE_"+str(measure_count)
    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]

    if current_threshold_column_name not in pm_column_names:
        pm_columns.AddCalculatedColumn(current_threshold_column_name, "["+measurename.strip()+"]")
    else:
        Document.Data.Tables[pm_table_name].Columns[current_threshold_column_name].As[CalculatedColumn]().Expression= "["+measurename+"]"

def add_update_current_time_stamp_column(pm_table_name,alarm_type,alarm_table_ext):

    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]

    if alarm_table_ext=='_DAY':
        calc_col = "Max([DATE_ID])"
    else:
        calc_col = "Max([DATETIME_ID])"

    current_date_stamp_Column_Name="CURRENT_DATESTAMP"
    if current_date_stamp_Column_Name not in pm_column_names:
        pm_columns.AddCalculatedColumn(current_date_stamp_Column_Name, calc_col)
    else:
        Document.Data.Tables[pm_table_name].Columns[current_date_stamp_Column_Name].As[CalculatedColumn]().Expression= calc_col

def add_update_time_column(pm_table_name,alarm_type,aggregation, alarm_table_ext, alarm_name):

    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]
    if alarm_table_ext=='_DAY':
        calc_col = "Date([DATE_ID])"
        date_column_name = 'Date'
    else:
        calc_col = "Time([DATETIME_ID])"
        date_column_name="TIME"
    if date_column_name not in pm_column_names:
        pm_columns.AddCalculatedColumn(date_column_name, calc_col)
    else:
        Document.Data.Tables[pm_table_name].Columns[date_column_name].As[CalculatedColumn]().Expression= calc_col


def add_alarm_type_column(pm_table_name,alarm_type):
    alarm_type_name='Alarm Type'
    pm_columns = Document.Data.Tables[pm_table_name].Columns
    found,column=Document.Data.Tables[pm_table_name].Columns.TryGetValue(alarm_type_name)

    if found:
        Document.Data.Tables[pm_table_name].Columns.Remove(alarm_type_name)
        pm_columns.AddCalculatedColumn(alarm_type_name, "'"+alarm_type+"'")
    else:
        pm_columns.AddCalculatedColumn(alarm_type_name, "'"+alarm_type+"'")


def template_columns(template_table_name):

    """Retrieves calculated type of columns from template and returns them in a list - Pm Alarming

    Arguments:
        template_table_name {string} -- template table name corresponding to alarm type
    """
    template_calculated_cols=[]
    template_table = Document.Data.Tables[template_table_name]
    template_cols = Document.Data.Tables[template_table_name].Columns

    for col in template_cols:
        col_name=str(col)
        calculated_col = template_table.Columns.Item[col_name]
        col_properties= calculated_col.Properties
        column_type=col_properties.GetProperty("ColumnType")
        if str(column_type) == 'Calculated':
            template_calculated_cols.append(col_name)

    return template_calculated_cols

def apply_template(alarm_name):
    """Applies corresponding alarm template and its' intermediate calculated columns to the data table - Pm Alarming

    Arguments:
        alarm_name {string} -- 'AlarmName' from Alarm Definitions table
        alarm_type {string} -- 'AlarmType' from Alarm Definitions table
    """
    condition=alarm_details[alarm_name]['condition']
    if "EMPTY" not in condition.upper() :
        threshold_value=alarm_details[alarm_name]['threshold_value']
        template_table_name="ThresholdTemplate"
        template_table = Document.Data.Tables[template_table_name]

        alarm_name = alarm_name.strip()
        complete_table_name=alarm_name

        template_calculated_columns=[]

        template_calculated_columns = template_columns(template_table_name)

        for col in template_calculated_columns:
            col_name=str(col)
            template_column = template_table.Columns.Item[col_name]
            col_properties= template_column.Properties
            column_type=col_properties.GetProperty("ColumnType")
            if str(column_type) == 'Calculated':
                column_data_type=col_properties.GetProperty("DataType")
                placeholder_expression = PLACEHOLDER_EXPRESSIONS[str(column_data_type)]
                table_cols = Document.Data.Tables[complete_table_name].Columns
                table_cols_names = [column.Name for column in table_cols]
                if col not in table_cols_names:
                    table_cols.AddCalculatedColumn(col, placeholder_expression)

        for col in template_calculated_columns:
            col_name=str(col)
            template_column = template_table.Columns.Item[col_name]
            col_properties= template_column.Properties
            column_type=col_properties.GetProperty("ColumnType")
            if str(column_type) == 'Calculated':
                expression=col_properties.GetProperty("Expression")
                expression=expression.replace(']>[',']'+condition+'[')
                Document.Data.Tables[complete_table_name].Columns[col_name].As[CalculatedColumn]().Expression= expression
        Document.Data.Tables[complete_table_name].Columns['THRESHOLD_1'].As[CalculatedColumn]().Expression= threshold_value


def get_calculated_columns(table_name):
    """gets a dictionary of calulated columns and their expressions

    Arguments:
        table_name -- Name of data table to retrieve calculated columns from
    Returns:
        calc_columns_dict -- String dict of calulated columns and their expressions
        calc_types_dict -- String dict of calulated columns and their data types
    """

    calc_columns_dict = {}
    calc_types_dict = {}
    data_table = Document.Data.Tables[table_name]
    column_collection = data_table.Columns

    for column in column_collection:
        if column.Properties.ColumnType == DataColumnType.Calculated:
            calc_col = column.Properties.GetProperty("Expression")

            if column.Properties.DataType.ToString() == "String":
                calc_col = calc_col.replace("\"","'")

            calc_columns_dict[column.Name] = calc_col
            calc_types_dict[column.Name] = column.Properties.DataType.ToString()

    return calc_columns_dict, calc_types_dict


def get_alarm_id(alarm_name):
    """Gets the current Alarm_ID

    Arguments:
        None
    Returns:
        Alarm_ID
    """
    alarm_definitions_table_name = 'Alarm Definitions'
    filtercondition="[AlarmName]= '" + alarm_name + "'"
    alarm_def_dt, alarm_def_cursor, alarm_def_index=get_spotfire_dt(alarm_definitions_table_name,filtercondition)
    alarm_ID = 0
    for _ in alarm_def_dt.GetRows(alarm_def_index, Array[DataValueCursor](alarm_def_cursor.values())):
        if alarm_name == alarm_def_cursor['AlarmName'].CurrentValue:
            alarm_ID = alarm_def_cursor['AlarmID'].CurrentValue

    return alarm_ID

def create_value_list_for_sql(alarm_dict, column_list):
    """ create a string in the format of (?,?,?)etc. so that the correct amount of command parameters can be added."""
    overall_rows = []
    for sql_column in alarm_dict.items():
        value_list = []
        current_row = ""

        for i in range(len(column_list)):
            value_list.append('?')

        current_row = """({0})""".format(','.join(value_list))
        overall_rows.append(current_row)

    return ','.join(overall_rows)

def apply_parameters(command, query_parameters, column_list):
    """ for an ODBC command, add all the required values for the parameters."""

    parameter_index = 0

    for col,col_value in query_parameters.items():
        # need to be added in correct order, so use the column_list to define the order
        for column_name, odbc_col_type in column_list.items():
            command.Parameters.Add("@col"+str(parameter_index), odbc_col_type).Value = str(col_value[column_name])
            parameter_index += 1

    return command

def run_netan_sql_param(sql,query_parameters, column_list):
    """ Run a SQL query using ODBC connection """

    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, query_parameters, column_list)
        command.ExecuteNonQuery()

        connection.Close()
    except Exception as e:
        print(e.message)
        raise


def insert_alarm_formulas(alarm_name):
    """ inserts formulas in to tblAlarmFormulas

    Arguments:
        alarm_name -- Name of alarm used in SQL query
    Returns:
        none
    """
    paramater_list = {}
    alarm_ID = get_alarm_id(alarm_name)

    columns_for_insert_dict = OrderedDict(
        [
            ("AlarmName",OdbcType.VarChar),
            ("AlarmColumnName", OdbcType.VarChar),
            ("AlarmColumnFormula", OdbcType.VarChar),
            ("AlarmID", OdbcType.Int),
            ("AlarmColumnDataType", OdbcType.VarChar)
        ]
    )

    columns_for_insert = ["""\"{0}\"""".format(column) for column in columns_for_insert_dict]

    sql_query = """INSERT INTO "tblAlarmFormulas" ({0}) VALUES """.format(','.join(columns_for_insert))

    calc_columns_dict, calc_types_dict = get_calculated_columns(alarm_name)

    parameter_index = 0
    for column_name, column_expression in calc_columns_dict.items():
        paramater_list[parameter_index] = {
            "AlarmName": alarm_name,
            "AlarmColumnName": column_name,
            "AlarmColumnFormula": column_expression,
            "AlarmID": alarm_ID,
            "AlarmColumnDataType": calc_types_dict[column_name]
        }
        parameter_index += 1

    sql_query += create_value_list_for_sql(paramater_list, columns_for_insert)

    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)


def remove_table(table_name):
    """Removes the newly created table from fetch data

    Arguments:
        alarm_name -- Name of alarm used in SQL query
    Returns:
        None
    """
    data_table = Document.Data.Tables[table_name]
    if data_table:
        Document.Data.Tables.Remove(data_table)



def getEniqId(eniqName):
    eniqId = ""
    table=Document.Data.Tables["EniqEnmMapping"]
    rowSelection=table.Select('EniqName = "'+eniqName+'"')
    cursor = DataValueCursor.CreateFormatted(table.Columns["EniqID"])
    for row in table.GetRows(rowSelection.AsIndexSet(),cursor):
        eniqId = cursor.CurrentValue
        break
    return eniqId



def update_alarm_query(alarm_name,measure):
    ''' write the alarm query to the database '''
    table_type = alarm_details[alarm_name]["alarm_table_ext"].replace('_','')
    paramater_list = {}

    sql_query = '''
                UPDATE "tblAlarmDefinitions"
                SET "MeasureName" = ?
                ,"AlarmQuery" = ?
                ,"PeriodDuration" = ?
                ,"TableType" = ?
                ,"TableName" = ?
                ,"LookBackVal" = ?
                ,"LookBackUnit" = ?
                ,"DataRangeVal" = ?
                ,"DataRangeUnit" = ?
                WHERE "AlarmName" = ?;
                '''
    columns_for_insert_dict = OrderedDict(
        [
            ("MeasureName", OdbcType.VarChar),
            ("AlarmQuery", OdbcType.VarChar),
            ("PeriodDuration", OdbcType.VarChar),
            ("TableType", OdbcType.VarChar),
            ("TableName", OdbcType.VarChar),
            ("LookBackVal", OdbcType.VarChar),
            ("LookBackUnit", OdbcType.VarChar),
            ("DataRangeVal", OdbcType.VarChar),
            ("DataRangeUnit", OdbcType.VarChar),
            ("AlarmName",OdbcType.VarChar)
        ]
    )


    paramater_list['update_query'] = {
                                      "MeasureName":measure,
                                      "AlarmQuery":alarm_details[alarm_name]["query"],
                                      "PeriodDuration":alarm_details[alarm_name]["period_duration"],
                                      "TableType":table_type,
                                      "TableName":alarm_details[alarm_name]["table_name_db"],
                                      "LookBackVal":alarm_details[alarm_name]["look_back_val"],
                                      "LookBackUnit":alarm_details[alarm_name]["look_back_unit"],
                                      "DataRangeVal":alarm_details[alarm_name]["data_range_val"],
                                      "DataRangeUnit":alarm_details[alarm_name]["data_range_unit"],
                                      "AlarmName":alarm_name
                                 }
    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)

def convert_formula_format(raw_formula, measure_name,table_names):

    kpiformula = set()
    if '_EUTRANCELLFDD_' not in table_names or '_EUTRANCELLTDD_' not in table_names:    
        rawformula = raw_formula
        rawformula = [counter.replace(".", "_") for counter in rawformula]
        
        for formula, kpi_name in zip(rawformula, list(measure_name)):
                
            #creating the raw formula into the same format as kpi formula in order for other functions to handle it      
            kpi = remove_group_functions(formula)
            kpiformula.add(kpi)
    else:
        kpiformula=raw_formula

    return kpiformula


def migrateAlarmToMultiEniq():
    time.sleep(5)
    #function adds selected eniq info to the alarms in netanDb
    eniqID = getEniqId(eniq_name)
    paramater_list = {}
    sql_query = '''
    UPDATE "tblAlarmDefinitions"
    SET "EniqID"  = ?
    WHERE "EniqID" is null;
    '''
    columns_for_insert_dict = OrderedDict([("EniqID", OdbcType.Int)])
    paramater_list['update_query'] = {"EniqID":eniqID}
    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)
    Document.Data.Tables["Alarm Definitions"].Refresh()
    
     
 

def migrateCollectionToMultiEniq():
    #function adds selected eniq info to the alarms in netanDb
    time.sleep(5)
    eniqID = getEniqId(eniq_name)
    paramater_list = {}
    sql_query = '''
    UPDATE "tblCollection"
    SET "EniqID"  = ?
    WHERE "EniqID" is null;
    '''
    columns_for_insert_dict = OrderedDict([("EniqID", OdbcType.Int)])
    paramater_list['update_query'] = {"EniqID":eniqID}
    run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)
    Document.Data.Tables["NodeCollection"].Refresh() 


@timeit
def main():
    ''' main function to create tables from queries'''

    try:
        queries_from_alarm_list(alarm_details)
    except Exception as e:
        notify.AddWarningNotification("Exception","Error in migration of Alarms",str(e))
    
    for idx , (alarm_name, query) in enumerate(alarm_for_template):
        try:
            ps.CurrentProgress.ExecuteSubtask('Migrating Alarm Rule %s of %s...' % (idx+1,len(alarm_for_template)))
            ps.CurrentProgress.ExecuteSubtask('Processing Alarm Rule %s ' % (alarm_name))
            alarm_type=alarm_details[alarm_name]['alarm_type']
            aggregation=alarm_details[alarm_name]['aggregation']
            alarm_table_ext=alarm_details[alarm_name]['alarm_table_ext']
            source_table_name = alarm_name
            alarm_table_name=alarm_name
           
            if run_query(source_table_name,query,alarm_name):
                Document.Data.Tables[alarm_table_name].Refresh()

                if alarm_name in kpis:

                    if aggregation == "1 Hour":
                        add_datetime_calc_column(alarm_table_name)
                    updated_measure_name_list = []
                    for measurename,measuredata in [e for e in kpis[alarm_name].items()]:
                        if checkIfDuplicates([c.split(".")[1] for c in alarm_details[alarm_name]['counters']]):
                            if 'UNION'in alarm_details[alarm_name]['formula']:						
                                temp_set = set()
                                temp_set.add(measuredata['kpiformula'])	
                                kpiformula = convert_formula_format(temp_set,alarm_details[alarm_name]['measurename'],alarm_details[alarm_name]['table_names'])
                                add_kpi_calc_column(alarm_table_name, measurename, ''.join(kpiformula))	
                            else:
                                kpiformula = convert_formula_format(alarm_details[alarm_name]['rawformula'],alarm_details[alarm_name]['measurename'],alarm_details[alarm_name]['table_names'])
                                add_kpi_calc_column(alarm_table_name, measurename, ''.join(kpiformula))	
                        else:								
                            add_kpi_calc_column(alarm_table_name, measurename, measuredata['kpiformula'])
                        add_measure_value_column(alarm_table_name, measuredata['kpi_order'], measurename)
                        add_update_current_time_stamp_column(alarm_table_name, alarm_type, alarm_table_ext)
                        add_update_time_column(alarm_table_name, alarm_type, aggregation, alarm_table_ext, alarm_name)
                        updated_measure_name_list.append(update_kpi_name(measurename))

                    add_alarm_type_column(alarm_table_name, alarm_type)
                    apply_template(alarm_name)
                    insert_alarm_formulas(alarm_name)
                    update_alarm_query(alarm_name,(';'.join(updated_measure_name_list)))
                    remove_table(alarm_name)
           
        except Exception as e:
            notify.AddWarningNotification("Exception","Error in migrating Alarm "+ alarm_name ,str(e))
            print("Exception: ",e)


alarm_details=get_alarm_details()



try:
    ps.ExecuteWithProgress('Migration of Alarms is in progress', 'Adding data Source to Alarms Definitons', migrateAlarmToMultiEniq)
    ps.ExecuteWithProgress('Migration of Alarms is in progress', 'Adding data Source to Node Collections', migrateCollectionToMultiEniq)
except Exception as e:
    notify.AddWarningNotification("Exception","Error in adding Eniq data source to database ", "Eniq Data Source not Added")


if alarm_details:
    ps.ExecuteWithProgress('Migration of Alarms is in progress', 'This will take some time based on number of Alarms to migrate', main)

        
else:
    notify.AddWarningNotification("Exception","There is no alarm to migrate.", "There is no alarm to migrate.")




