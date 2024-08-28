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
# Name    : FetchData.py
# Date    : 29/08/2021
# Revision: 4.0
# Purpose :
#
# Usage   : PM Alarming 
#


# General imports
import time
import datetime
from datetime import datetime, timedelta
from System import Array, Object
from System.Collections.Generic import Dictionary, List
from itertools import combinations,tee
import sys
import re
import collections
from collections import OrderedDict
import clr

clr.AddReference('System.Data')
from System.Data.Odbc import OdbcConnection,OdbcType
from System.Data.Odbc import OdbcConnection, OdbcDataAdapter
from System.Data import DataSet
import collections
from calendar import monthrange

# Spotifre imports
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Framework.Library import *
from Spotfire.Dxp.Data.DataOperations import *
from Spotfire.Dxp.Application.Visuals import TablePlot, VisualTypeIdentifiers, LineChart, CrossTablePlot, HtmlTextArea
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from Spotfire.Dxp.Framework.ApplicationModel import ApplicationThread, ProgressService, ProgressCanceledException
from Spotfire.Dxp.Application.Layout import LayoutDefinition
from System import DateTime

# Global vars
ps = Application.GetService[ProgressService]()
notify = Application.GetService[NotificationService]()



connString = "DSN=" + Document.Properties['ENIQDataSourcesDropDown']+";Pooling=true;Max Pool Size=20;Enlist=true;FetchArraySize=100000;"

if Document.Properties["IsEdit"] == "Edit":
    connString = "DSN=" + Document.Properties['EniqName']+";Pooling=true;Max Pool Size=20;Enlist=true;FetchArraySize=100000;"


kpis = {}

alarm_for_db = []

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

dateFormats = {}
dateFormats['DATETIME_ID'] = "yyyy'-'MM'-'dd' 'HH':'mm"
dateFormats['DATE_ID'] = "yyyy'-'MM'-'dd"

templates_dict={
    'cd':'CdTemplate', 
    'pcd':'PcdTemplate', 
    'dynamic':'DynamicTemplate', 
    'trend':'TrendTemplate', 
    'pcd+cd':'CdPcdTemplate',
    'threshold':'ThresholdTemplate',
    'cdt': 'CdtTemplate'
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
'pcd': {'none': {'15': {'ROP': {'lookback_val':15, 'present_min':'15', 'constant':15},
                        'HOUR': {'lookback_val':60, 'present_min':'15', 'constant':15},
                        'DAY': {'lookback_val':1440, 'present_min':'15', 'constant':15}},
                  '5': {'ROP': {'lookback_val':5, 'present_min':5, 'constant':5},
                        'HOUR': {'lookback_val':60, 'present_min':5, 'constant':5},
                        'DAY': {'lookback_val':1440, 'present_min':5, 'constant':5}}},
        'hour': {'15': {'ROP': {'lookback_val':15, 'present_min':60, 'constant':60},
                        'HOUR': {'lookback_val':60, 'present_min':60, 'constant':60},
                        'DAY': {'lookback_val':1440, 'present_min':60, 'constant':60}},
                  '5': {'ROP': {'lookback_val':15, 'present_min':60, 'constant':60},
                        'HOUR': {'lookback_val':60, 'present_min':60, 'constant':60},
                        'DAY': {'lookback_val':1440, 'present_min':60, 'constant':60}}},
        'day': {'1440': {'DAY': {'lookback_val':1, 'present_min':'1', 'constant':1}}}},

'threshold': {'none': {'present_min':15},
              'hour': {'present_min':60},
              'day': {'present_min':0}}
}

data_range_dict = { 

'dynamic': {'none': {'15': {'ROP': {'data_range_val':15, 'present_min':'15', 'constant':15},
                            'HOUR': {'data_range_val':60, 'present_min':'15', 'constant':15},
                            'DAY': {'data_range_val':1440, 'present_min':'15', 'constant':15}},
                     '5':  {'ROP': {'data_range_val':5, 'present_min':'5', 'constant':5},
                            'HOUR': {'data_range_val':60, 'present_min':'5', 'constant':5},
                            'DAY': {'data_range_val':1440, 'present_min':'5', 'constant':5}}},
            'hour': {'15': {'ROP': {'data_range_val':15, 'present_min':'60', 'constant':60},
                            'HOUR': {'data_range_val':60, 'present_min':'60', 'constant':60},
                            'DAY': {'data_range_val':1440, 'present_min':'60', 'constant':60}},
                     '5':  {'ROP': {'data_range_val':5, 'present_min':'60', 'constant':60},
                            'HOUR': {'data_range_val':60, 'present_min':'60', 'constant':60},
                            'DAY': {'data_range_val':1440, 'present_min':'60', 'constant':60}}},
            'day': {'1440': {'DAY': {'data_range_val':1, 'present_min':'1', 'constant':1}}}},

'cd': {'none': {'15': {'ROP': {'data_range_val':15},
                     'HOUR': {'data_range_val':60},
                     'DAY': {'data_range_val':1440}},
                '5': {'ROP': {'data_range_val':5},
                     'HOUR': {'data_range_val':60},
                     'DAY': {'data_range_val':1440}}},
     'hour': {'15': {'ROP': {'data_range_val':15},
                     'HOUR': {'data_range_val':60},
                     'DAY': {'data_range_val':1440}},
                '5': {'ROP': {'data_range_val':5},
                     'HOUR': {'data_range_val':60},
                     'DAY': {'data_range_val':1440}}},
     'day': {'1440': {'DAY': {'data_range_val':1}}}}
}

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

ESS_TABLE_TOPOLOGY_MAPPING = {'DC_E_ERBS_EUTRANCELLFDD_DAY': 'eUtranCellRef_DN', 'DC_E_ERBS_EUTRANCELLTDD_DAY': 'eUtranCellRef_DN', 'DC_E_NR_NRCELLDU_DAY':'gUtranCellRelationRef_DN', 'DC_E_ERBS_SHARINGGROUP_DAY':'sharingGroup_DN', 'DC_E_NR_GNBDUFUNCTION_DAY':'gUtranCellRelationRef_DN','DC_E_NR_NRCELLDU_V_DAY':'gUtranCellRelationRef_DN',
                              'DC_E_ERBS_EUTRANCELLFDD_RAW': 'eUtranCellRef_DN', 'DC_E_ERBS_EUTRANCELLTDD_RAW': 'eUtranCellRef_DN', 'DC_E_NR_NRCELLDU_RAW':'gUtranCellRelationRef_DN', 'DC_E_ERBS_SHARINGGROUP_RAW':'sharingGroup_DN', 'DC_E_NR_GNBDUFUNCTION_RAW':'gUtranCellRelationRef_DN','DC_E_NR_NRCELLDU_V_RAW':'gUtranCellRelationRef_DN'}
ess_topology_table = 'DIM_E_LTE_SHARINGGRP'


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
    result = expr
    result = strip_table_names(expr)
    result = remove_group_functions(result)
    result = add_brackets_to_variables(result)
    return convert_is_null(result)


def create_cursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""

    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))
    return cusrDict


def get_the_node_list(ne_collection):
    nodes_sub = []
    subnetworkTable = Document.Data.Tables['SubNetwork List']
    nodeType = Document.Properties['NodeType']
    subnetwork_cur = create_cursor(subnetworkTable)
    subnetwork_filter = subnetworkTable.Select("[SubnetworkName] = '" + ne_collection + "' and [NodeType] = '" + nodeType + "' and [DataSourceName] = '" + Document.Properties['ENIQDataSourcesDropDown']+ "'")
    for node in subnetworkTable.GetRows(subnetwork_filter.AsIndexSet(), Array[DataValueCursor](subnetwork_cur.values())):
        nodes_sub.append(subnetwork_cur['NodeName'].CurrentValue)
    if len(nodes_sub) == 0:
        print "inside if"
        nodes_sub.append('')  
    node_list = ','.join("'{}'".format(i) for i in nodes_sub)
    
    
    return node_list

def getTopologyTableData(nodeType, dataS):

    topologyTableName = 'Modified Topology Data'
    topologyDataTable = Document.Data.Tables[topologyTableName]
    topologyDataTableCur = create_cursor(topologyDataTable)

    selectedNodeType = topologyDataTable.Select("[Node]= '" + nodeType + "'" )

    for node in topologyDataTable.GetRows(selectedNodeType.AsIndexSet(), Array[DataValueCursor](topologyDataTableCur.values())):
        serverName = topologyDataTableCur['DataSourceName'].CurrentValue
        if serverName == dataS:
			tableName = topologyDataTableCur['Topology Table'].CurrentValue
			FDNName = topologyDataTableCur['FDN Key'].CurrentValue
			KeyName = topologyDataTableCur['Key'].CurrentValue
			print tableName
    return tableName + ',' + FDNName + ',' + KeyName	
def runQuery(sql):
    try:
        connString = "DSN=" + Document.Properties['ENIQDataSourcesDropDown']
        connection = OdbcConnection(connString)

        dataSet = DataSet()
        start = time.time()
        connection.Open()
        adaptor = OdbcDataAdapter(sql, connection)
        dataSet = DataSet()
        adaptor.Fill(dataSet)
        connection.Close()
        end = time.time()
        print "overall:" + str(end - start)

        return dataSet
    except Exception as e:
        print(e)
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    
def wildcardQuery(ne_collection):
    active_collection = node_collections_data_table.Select("[CollectionName] ='" + ne_collection + "'")
    for nodes in node_collections_data_table.GetRows(active_collection.AsIndexSet(), Array[DataValueCursor](node_collection_cur.values())):
	     Collection_type=node_collection_cur['CollectionType'].CurrentValue
	     SystemArea=node_collection_cur['SystemArea'].CurrentValue
	     NodeType=node_collection_cur['NodeType'].CurrentValue
	     EniqName=node_collection_cur['EniqName'].CurrentValue
	     WildcardExpression=node_collection_cur['WildCardDefinition'].CurrentValue

    if '_' in WildcardExpression:
	     wildcardExpressionFinal = WildcardExpression.replace("_","[_]")
    else:
	     wildcardExpressionFinal = WildcardExpression
		 
    alldata = getTopologyTableData(NodeType,EniqName).split(',')
    tableName = alldata[0]
    FDNName = alldata[1]
    KeyName = alldata[2]
    sql = """
    SELECT DISTINCT 
        {0} as NodeName,
        {1} as FDN, 
        '{2}' AS NodeType, 
        '{3}' as SystemArea,
        '{4}' as CollectionName,
        '{5}' as WildcardExpression
    FROM 
        {6}
    where
	{7}""".format(KeyName,FDNName,NodeType,SystemArea,ne_collection, WildcardExpression.replace("'", "''"),tableName, wildcardExpressionFinal)
    #print(sql)    
    dataSet = runQuery(sql)
    #print dataSet
    nodenames = [row[0] for row in dataSet.Tables[0].Rows]
    return nodenames
    
    
def get_node_list(ne_collection):
    '''Return a list of nodes in a node collection (or just a single node if no collection)'''
    if Document.Properties['SingleOrCollection'] == 'Subnetwork':
        node_list = get_the_node_list(ne_collection)
        if node_list == "":
            Document.Properties['SubNetworkNoNode'] = "Selected Subnetwork having 0 Nodes!"
        #print "Node_List"
        #print node_list
    else:
        ne_list = []
        active_collection = node_collections_data_table.Select("[CollectionName] ='" + ne_collection + "'")

        for nodes in node_collections_data_table.GetRows(active_collection.AsIndexSet(), Array[DataValueCursor](node_collection_cur.values())):
			Collection_type=node_collection_cur['CollectionType'].CurrentValue
			if(Collection_type=='Dynamic'):
				ne_list=wildcardQuery(ne_collection)
			else:
				ne_list.append(node_collection_cur['NodeName'].CurrentValue)				 
				

			
			

        # check if its a single node (i.e. an empty list because node not included in collection list)
        if not ne_list:
			node_list = "'" + ne_collection + "'"
        else:
			node_list = ','.join("'{}'".format(i) for i in ne_list)

    


    return node_list


def remove_tablename_prefix(cols):
    tmp = cols.split(',')
    columns= list(set(map(lambda x: x.split('.')[-1], tmp)))
    return columns


def replaceROPS(replaceString, aggLevel, aggVal):

    if aggLevel == 'None' or aggLevel == 'raw':
        replaceString = re.sub(r'<ROPS>/(15|((AVG|MAX)\(\w*\.PERIOD_DURATION\)))' ,'1', replaceString, flags=re.IGNORECASE)
    else:
        replaceString  = replaceString.replace('<ROPS>', aggVal)
    return replaceString


def get_period_duration(table_name):
    """
    Gets the Period Duration from ENIQ DB data table based on KPIs/counters selection

    Arguments:
        table_name {string} -- PM Data table name
    """
    try:
        query='SELECT max(PERIOD_DURATION) as "PERIOD_DURATION" FROM  ' + table_name
        db_settings = DatabaseDataSourceSettings("System.Data.Odbc", connString, query)
        ds = DatabaseDataSource(db_settings)
        new_data_table = Document.Data.Tables.Add('temp',ds)
        table=Document.Data.Tables['temp']
        cursor = DataValueCursor.CreateFormatted(table.Columns["PERIOD_DURATION"])
        val_data = List [str]();

        for row in table.GetRows(cursor):
            value = cursor.CurrentValue
            if value <> str.Empty:
                val_data.Add(value)

        val_data = List [str](set(val_data))
        val_data = ' '.join(val_data).split()
        val_data = ' '.join(val_data).split()
        Document.Properties['PeriodDuration'] = val_data[0]
        Document.Data.Tables.Remove(new_data_table)

        period_duration = ""
        if val_data[0] == "(Empty)":
            period_duration = "15"
        else:
            period_duration = val_data[0]
        return period_duration

    except Exception as e:
        notify.AddWarningNotification("Exception","DataBase Connection Not SetUp",str(e))
        print("Exception: ", e)


def change_remove_table_extension(item, table_extension_check):
    """Changes/removes PM data table sufix from RAW/DELTA/DAY tables

    Arguments:
        item {string} -- Item name. Item can be table name/counter with table name
        case {string} -- case number. Directs which conversion to use
    Returns:
        item {string} -- Item with replaced/removed sufix
    """
    
    if table_extension_check == 'add_day_ext':
        item = item.replace('_RAW', '_DAY').replace('_DELTA', '_DAY')
    elif table_extension_check == 'remove_ext':
        item = item.replace('_RAW', '').replace('_DELTA', '')

    return item


def check_measures_selected_error(alarm_type, alarm_measure_list):
    
    """check if measure values selected match expressions in template"""

    alarm_template = templates_dict[alarm_type.lower()]
    template_cols = Document.Data.Tables[alarm_template].Columns
    count_measure_val_selected = len([i for i in alarm_measure_list if i != ""])
    count_measure_val_selected +=1
    measures_selected= ['MEASUREVALUE_' + str(measure_num) for measure_num in range(1,count_measure_val_selected)]

    for template_col in template_cols:
        if template_col.Properties.ColumnType == DataColumnType.Calculated:
            calc_col = Document.Data.Tables[alarm_template].Columns[template_col.Name].As[CalculatedColumn]()

            if 'MEASUREVALUE_' in calc_col.Expression:
                column_measures = re.findall('MEASUREVALUE_[1-4]', calc_col.Expression, re.DOTALL)

                if column_measures:
                    measures_not_selected = set([cm for cm in column_measures if cm not in measures_selected])
                    if measures_not_selected:
                        return (','.join(measures_not_selected)  +" used in " +alarm_template+" expressions but not selected in Measure Details.")
                        
               
    return ''
    

def get_sorted_list(alarm_measures_list,measure_table_cursor,node_type,eniq_ds):
    temp_list = []
    temp_type_dict = OrderedDict()
    measure_mapping_data_table = Document.Data.Tables['Measure Mapping']
    for alarm in alarm_measures_list:
        measure_table_filter = measure_mapping_data_table.Select("[Measure]= '" + alarm + "' AND [Node Type]= '"+node_type+ "' AND [DataSourceName]= '"+eniq_ds+"'")
        for measure in measure_mapping_data_table.GetRows(measure_table_filter.AsIndexSet(), Array[DataValueCursor](measure_table_cursor.values())):
            temp_type_dict[alarm] = measure_table_cursor['Measure Type'].CurrentValue
            
    if 'Counter' in temp_type_dict.values() and ('KPI' in temp_type_dict.values() or 'RI' in temp_type_dict.values()):
        for key, value in temp_type_dict.items():
            if value == 'KPI' or value == 'RI':
                temp_list.append(key)
        left_itmes = [x for x in temp_type_dict.keys() if x not in temp_list]
        for x in left_itmes:
            temp_list.append(x)
    else:
        temp_list = alarm_measures_list
        
    #print "temp_list", temp_list
        
    return temp_list
            
            

def get_alarm_details():
    '''
       Creates and returns a dictionary with all details of the active alarm(s) in Alarm Definitions table
    '''
    alarm_rules = {}

    alarm_defintions_table_name = 'Alarm Definitions'
    alarm_defintions_data_table = Document.Data.Tables[alarm_defintions_table_name]
    alarm_table_cursor = create_cursor(alarm_defintions_data_table)

    measure_mapping_table_name = 'Measure Mapping'
    measure_mapping_data_table = Document.Data.Tables[measure_mapping_table_name]
    measure_table_cursor = create_cursor(measure_mapping_data_table)

    #marked_row_selection = Document.ActiveMarkingSelectionReference.GetSelection(alarm_defintions_data_table).AsIndexSet()
    measure_name_col = 'MeasureName'
    table_name = Document.Properties["TableName"]
    table_name = table_name.upper()
    all_tables_list = table_name.split(',')
    table_name = table_name.split(',')[0]
    aggregation = Document.Properties['Aggregation']
    node_type = Document.Properties['NodeType']
    eniq_ds = Document.Properties['ENIQDataSourcesDropDown']
    table_type = 'RAW'
    Document.Properties['TableClass'] = table_type
    if '_DELTA' in table_name:
        table_type = 'DELTA'
        Document.Properties['TableClass'] = table_type

    if aggregation == '1 Day':
        table_name = change_remove_table_extension(table_name,'add_day_ext')
    # for each active alarm get the alarm details for the dictionary key
 
    alarm_measure_list = []
    alarm_measure_list.append(Document.Properties['SelectedKPI1'])
    alarm_measure_list.append(Document.Properties['SelectedKPI2'])
    alarm_measure_list.append(Document.Properties['SelectedKPI3'])
    alarm_measure_list.append(Document.Properties['SelectedKPI4'])
    complete_alarm_measure = ";".join(alarm_measure_list)
    alarm_measure = ";".join(alarm_measure_list)
    alarm_name = Document.Properties['AlarmName']
    alarm_name = alarm_name.strip()

    if Document.Properties["SingleOrCollection"] == "Single Node":
        ne_collection = Document.Properties["SingleNodeValue"]
    elif Document.Properties["SingleOrCollection"] == "Subnetwork":
        ne_collection = Document.Properties['subnetwork']
    else:
        ne_collection = Document.Properties['NECollection']

    alarm_def_schedule = Document.Properties['Schedule']
    alarm_type = Document.Properties['AlarmType']
    aggregation = Document.Properties['Aggregation']
    
    if Document.Properties["IsEdit"] != "Edit":
        Document.Properties["ValidationError"] = check_measures_selected_error(alarm_type, alarm_measure_list)
    if aggregation == "1 Day": 
        alarm_table_ext = '_DAY'
    else:
        alarm_table_ext = '_RAW'

    if '_DELTA' in table_name and aggregation != '1 Day':
        alarm_table_ext = '_DELTA'  
    look_back_val = Document.Properties['LookbackVal']
    look_back_unit = Document.Properties['LookbackUnit']
    data_range_val = Document.Properties['DataRangeVal']
    data_range_unit = Document.Properties['DataRangeUnit']

   

    period_duration = get_period_duration(table_name)
    # there can be multiple kpis etc. for an alarm, so split them and then loop through them to get the counter details
    alarm_measures_list = alarm_measure.split(";")
    #removing empty list items after split
    alarm_measures_list = [alarm_measure for alarm_measure in alarm_measures_list if len(alarm_measure) > 0]
    kpi_order = 0
    alarm_measures_list = get_sorted_list(alarm_measures_list,measure_table_cursor,node_type,eniq_ds)
    
    for alarm_measure in alarm_measures_list:
        
        #here add the ENIQ_DS filter too. 
        measure_table_filter = measure_mapping_data_table.Select("[Measure]= '" + alarm_measure + "' AND [Node Type]= '"+node_type+ "' AND [DataSourceName]= '"+eniq_ds+"'")
        #measure_table_filter = measure_mapping_data_table.Select("[Measure]= '" + alarm_measure + "'")

        for measure in measure_mapping_data_table.GetRows(measure_table_filter.AsIndexSet(), Array[DataValueCursor](measure_table_cursor.values())):

            measure_name = measure_table_cursor['Measure'].CurrentValue
            counter_name = measure_table_cursor['Counters'].CurrentValue.replace(" ","")
            counters_names_list = counter_name.split(",")
            element = measure_table_cursor['ELEMENT'].CurrentValue.replace(" ","")
            measure_type = measure_table_cursor['Measure Type'].CurrentValue
            counter_type = measure_table_cursor['COLLECTIONMETHOD'].CurrentValue
            kpi_specific_where_clause = measure_table_cursor['WHERE CLAUSE'].CurrentValue       
            eniq_table_name = measure_table_cursor['TABLENAME'].CurrentValue.upper()
            keys_raw = measure_table_cursor['KEYS'].CurrentValue.split(',')
            keys_raw = [c.strip() for c in keys_raw]
            mapping_columns = measure_table_cursor['Mapping Columns'].CurrentValue
            
            # Modification S
            
            flex_value = measure_table_cursor['FlexFilterValues'].CurrentValue
            index_value = measure_table_cursor['Index'].CurrentValue
            
            # Modification E
            
            category = measure_table_cursor['Category'].CurrentValue
            raw_formula = measure_table_cursor['Formula'].CurrentValue.upper()
            time_aggregations = measure_table_cursor['TIMEAGGREGATION'].CurrentValue.split(",")
            group_aggregation = measure_table_cursor['GROUPAGGREGATION'].CurrentValue.split(",")
  
            time_col = time_columns[aggregation]
            counters = set()
            keys = set()
   
            if  time_col =='DATE_ID':
                raw_formula = replaceROPS(raw_formula, 'Day', '1440')
                counters_raw = get_variables(raw_formula)
                element = change_remove_table_extension(element,'add_day_ext')
                raw_formula = change_remove_table_extension(raw_formula,'add_day_ext')
                eniq_table_name = change_remove_table_extension(eniq_table_name,'add_day_ext')
                alarm_table_ext = change_remove_table_extension(alarm_table_ext,'add_day_ext')
                mapping_columns = change_remove_table_extension(mapping_columns,'add_day_ext')
                kpi_specific_where_clause = change_remove_table_extension(kpi_specific_where_clause,'add_day_ext')
               
                for c in counters_raw:
                    c = change_remove_table_extension(c,'add_day_ext')
                    counters.add(c)
                for k in keys_raw:
                    k = change_remove_table_extension(k,'add_day_ext')
                    keys.add(k)
            else:
                if aggregation == '1 Hour':
                    raw_formula = replaceROPS(raw_formula, 'Hour', '60')
                else:
                    raw_formula = replaceROPS(raw_formula, 'None', '')
                keys.update(keys_raw)
                counters = get_variables(raw_formula)
                
		   	
            if 'UNION' in counters:
                counters.remove('UNION')
            kpi_formula = spotfire_expression(raw_formula.split("UNION")[0])
            elements = element.split(',')
            keys.update(elements)
            key = alarm_name

            if aggregation == '1 Hour':
                time_val = "hour"
            elif aggregation == '1 Day':
                time_val = "day"
            else:
                time_val = "none"
				
            joins = []
            table_list = [c.strip() for c in (eniq_table_name.split(','))]
            if len(table_list)>1 and 'UNION' not in raw_formula:
                element_list = element.split(',')
                joins = get_common_joins(element_list,table_list,time_col)
            if category == 'ESS':
                for table in table_list:
                    joins.append(table + '.OSS_ID = ' + ess_topology_table + '.OSS_ID')
                    joins.append(table + '.SN = ' + ess_topology_table + '.NE_FDN')
                    joins.append(table + '.MOID = ' + ess_topology_table + '.' + ESS_TABLE_TOPOLOGY_MAPPING[table])
                table_list.append(ess_topology_table)
            if mapping_columns != '(Empty)':
                mapping_columns = mapping_columns.split(',') 
                joins = joins + mapping_columns		
            
            for c in counters:
				present_counter = c
            if  measure_type.strip() == 'Counter' and ('EUTRANCELLFDD' in present_counter.split('.')[0] or 'EUTRANCELLTDD' in present_counter.split('.')[0]) and len(all_tables_list)>=2:
                counter_added = present_counter.split('.')[1]
                table_added = present_counter.split('.')[0]
                if 'EUTRANCELLFDD' in table_added:
                    table_to_be_added = table_added.replace('EUTRANCELLFDD','EUTRANCELLTDD')
                    if table_to_be_added in all_tables_list:                
                        counters.add(table_to_be_added + '.' + counter_added)
                else:
                    table_to_be_added = table_added.replace('EUTRANCELLTDD','EUTRANCELLFDD')
                    if table_to_be_added in all_tables_list:
                        counters.add(table_to_be_added + '.' + counter_added)

            
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
                    # Modification
                    'flex_value' : flex_value,
                    'index_value' : index_value,
                    # Modification
                    'ne_collection': ne_collection,
                    'aggregation': aggregation,
                    'look_back_val': look_back_val,
                    'look_back_unit': look_back_unit,
                    'data_range_val': data_range_val,
                    'data_range_unit': data_range_unit,
                    'period_duration': period_duration,
                    'kpi_specific_where_clause': kpi_specific_where_clause,
                    'table_names' : (','.join(table_list)),
                    'join_keys': joins,
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
            alarm_rules[alarm_name]['rawformula'].add(raw_formula)
            alarm_rules[alarm_name]['counters'].update(counters)
            alarm_rules[alarm_name]['keys'].update(keys)

            if 'PERIOD_DURATION' in raw_formula:
                eniq_table_name_list = eniq_table_name.split(",")
                for name in eniq_table_name_list:
                    alarm_rules[alarm_name]['keys'].add(name+'.PERIOD_DURATION')
                    
        #for formula in alarm_rules[alarm_name]['rawformula']:
    alarm_rules[alarm_name]['formula'] = max(alarm_rules[alarm_name]['rawformula'], key=len) 
        
    #print "alarm_rules", alarm_rules
    return alarm_rules

def get_common_joins(element_list,table_list,time_col):
    """returns a list of common joins to be added in multi-table queries"""
    common_joins = []
    for i in range(len(element_list)-1):
        common_joins.append(element_list[i] + "=" + element_list[i+1])
    for num in range(0,len(table_list)-1,1):
        common_joins.append(table_list[num] + ".OSS_ID = " + table_list[num+1] + ".OSS_ID")
    for num in range(0,len(table_list)-1,1):
        common_joins.append(table_list[num] + "." + time_col + " = " + table_list[num+1] + "." + time_col)
    
    return common_joins

def loaded_date_sql(table_name,alarm_info):
    """get the latest loaded date for a given table. this is used as part of the main query for the date range."""
    original_table_name = table_name
    if 'ERBS' in table_name:
        g2_table_name = table_name.replace('ERBS_','ERBSG2_')
        table_name = "'" + table_name + "','" + g2_table_name + "'"
    else:
        table_name = "'" + table_name + "'"
    # generate sql query
    if alarm_info['alarm_table_ext'] in ['_RAW','_DELTA'] :
        sql = """declare @dateTime datetime
                select
                    @dateTime = (
                    select
                        max(datatime) as "datatime"
                    from
                        (
                        SELECT
                            max(datatime) as "datatime",
                            typename
                        FROM
                            LOG_LOADSTATUS
                        where
                            status = 'LOADED'
                            and typename in ({table})
                            and ROWCOUNT > 0
                group by typename) as sub)""".format(table=change_remove_table_extension(table_name, 'remove_ext'))
        if alarm_info['aggregation'] == '1 Hour' and alarm_info['period_duration'] == '5':
            sql += """
                SELECT
            @dateTime =
            CASE
                WHEN (
                SELECT
                    max({table}.PERIOD_DURATION)
                FROM
                    {table}) = 5 THEN (dateadd(mi,(-1-datepart(MINUTE,GETDATE())),GETDATE()))
                    """.format(table=original_table_name)
        else:
            sql += """
                    SELECT
                @dateTime =
                CASE
                    WHEN (
                    SELECT
                        max({table}.PERIOD_DURATION)
                    FROM
                        {table}) = 5 THEN (
                    SELECT
                        max({table}.DATETIME_ID)
                    FROM
                        {table})""".format(table=original_table_name)
        
        if alarm_info['aggregation'] == '1 Hour':
            sql += """
                    ELSE dateadd(mi,(-1-datepart(MINUTE,GETDATE())),GETDATE())
                    END
                """
        else:
            sql += """
                ELSE @dateTime
            END """

    elif alarm_info['alarm_table_ext'] == '_DAY':
        sql = """declare @dateTime datetime
                select
                    @dateTime = (
                    select
                        min(DATE_ID) as "DATE_ID"
                    from
                        (
                        SELECT
                            max(DATE_ID) as "DATE_ID",
                            typename
                        FROM
                            LOG_AggregationStatus
                        where
                            status = 'Aggregated'
                            and TIMELEVEL = 'DAY'
                            and typename in ({table})
                            and ROWCOUNT > 0
                        group by typename) as sub) """.format(table=table_name.replace('_DAY', ''))

    return sql

def loaded_date_sql_complex(alarm_info): 
    '''generates date time query for complex (union) KPIs because data may not be loaded for an MO involved in the KPI'''
    '''assumption - UNION is performed between 2 queries - needs to be updated further for more than 2 queries.'''
    count = 0
    date_query = []
    for formula_part in alarm_info["formula"].split('UNION'):
        count = count + 1
        table_set = set()
        counter_set = get_variables(formula_part)
        for counter in counter_set:
            table_set.add(counter.split(".")[0])
        date_query.append((loaded_date_sql((list(table_set))[0],alarm_info)).replace('@dateTime','@dateTime' + str(count)))
    date_query_complex_kpi = date_query[0] + date_query[1] + """declare @dateTime datetime 
                                                                select @dateTime = CASE 
                                                                WHEN (SELECT @dateTime1) IS NULL 
                                                                    THEN @dateTime2
                                                                ELSE 
                                                                    @dateTime1 
                                                                END """ 
    return date_query_complex_kpi

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

    if alarm_info["aggregation"] == '1 Day' and time_limit_check == 'past' and alarm_info["alarm_type"] in ['dynamic','pcd','pcd+cd']:
        operator_min = ">"
        operator_max = "<="

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


def get_time_limits(alarm_info):
    ''' get the time limits for the date clause fucntion based on lookback, schedule, data range and type of alarm'''
    time_limits = {}

    alarm_type = alarm_info["alarm_type"]
    time_val = alarm_info["time_val"]
    look_back_value = alarm_info['look_back_val']
    look_back_arg = alarm_info['look_back_unit']
    data_range_value = alarm_info['data_range_val']
    data_range_arg = alarm_info['data_range_unit']
    period_duration = alarm_info['period_duration']
    past_min='0'
    past_max=0

    if alarm_type == 'threshold' or alarm_type == 'cdt':
        if alarm_type == 'cdt':
            alarm_type = 'threshold'
        if period_duration == '5' and time_val == 'none':
            present_min = lookback_dict[alarm_type][time_val]['present_min']/3
        else:
            present_min = lookback_dict[alarm_type][time_val]['present_min']

    elif alarm_type == 'pcd':
        past_min = str((int(look_back_value)*lookback_dict[alarm_type][time_val][period_duration][look_back_arg]['lookback_val'])+lookback_dict[alarm_type][time_val][period_duration][look_back_arg]['constant'])
        past_max = int(look_back_value)*lookback_dict[alarm_type][time_val][period_duration][look_back_arg]['lookback_val']
        present_min = lookback_dict[alarm_type][time_val][period_duration][look_back_arg]['present_min']

    elif alarm_type == 'dynamic':
        past_min = str((int(data_range_value)*data_range_dict[alarm_type][time_val][period_duration][data_range_arg]['data_range_val'])+data_range_dict[alarm_type][time_val][period_duration][data_range_arg]['constant'])
        past_max = str(data_range_dict[alarm_type][time_val][period_duration][data_range_arg]['constant'])
        present_min = data_range_dict[alarm_type][time_val][period_duration][data_range_arg]['present_min']

    elif alarm_type == 'cd' or alarm_type == 'trend':
        if alarm_type == 'trend':
            alarm_type = 'cd'
        present_min = str((int(data_range_value)*data_range_dict[alarm_type][time_val][period_duration][data_range_arg]['data_range_val']))
        

    else:
        alarm_type_splitted = alarm_type.split('+')
        past_min = str((int(look_back_value)*lookback_dict[alarm_type_splitted[0]][time_val][period_duration][look_back_arg]['lookback_val'])+
        (int(data_range_value)*data_range_dict[alarm_type_splitted[1]][time_val][period_duration][data_range_arg]['data_range_val']))
        past_max = str((int(look_back_value)*lookback_dict[alarm_type_splitted[0]][time_val][period_duration][look_back_arg]['lookback_val']))
        present_min = str((int(data_range_value)*data_range_dict[alarm_type_splitted[1]][time_val][period_duration][data_range_arg]['data_range_val']))

    time_limits['present'] = {
        'min': str(present_min),
        'max': '0'
                }
    time_limits['past'] = {
        'min': str(past_min),
        'max': str(past_max)
    }    

    return time_limits


def get_aggregations(counters, table_type):
    """Gets a dict of counters used in query with the correct aggregation

    Arguments:
        counters -- list of counters used in SQL query

    Returns:
        counter_aggregations -- dictionary whewre key=counters, value=aggregation 
    """
    measure_mapping_table_name = 'Measure Mapping'
    measure_mapping_data_table = Document.Data.Tables[measure_mapping_table_name]
    measure_table_cursor = create_cursor(measure_mapping_data_table)
    counter_aggregations = {}

    for counter in counters:
        table_name = counter.split(".")[0]
        counter = counter.split(".")[1]
        counter_table_filter = ''
        if table_type in ['RAW','DELTA']:
            table_ext = '_' + table_type
            counter_table_filter = measure_mapping_data_table.Select("[Measure]= '" + counter + "." + table_name.replace('_DAY', table_ext) + "'")

        if counter not in measure_mapping_data_table.GetRows(counter_table_filter.AsIndexSet(), Array[DataValueCursor](measure_table_cursor.values())) and counter != 'PERIOD_DURATION':
            counter_aggregations[table_name+"."+counter] = ''

        for c in measure_mapping_data_table.GetRows(counter_table_filter.AsIndexSet(), Array[DataValueCursor](measure_table_cursor.values())):
            counter_name = ((measure_table_cursor['Measure'].CurrentValue).split('.'))[0]
            aggregation = measure_table_cursor['TIMEAGGREGATION'].CurrentValue

            if counter_name in counter:
                counter_aggregations[table_name+"."+counter_name] = aggregation
    return counter_aggregations


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


def aggregate_counters(counters, aggregation, table_type):
    """Adds an aggregation to the counters e.g SUM(PM_CELL_DOWNTIME_MAN)

    Arguments:
        counters -- list of aggregation used in SQL query
        aggregation -- aggregation used in SQL query
    Returns:
        counters --  updated list of counters with aggregations attached
    """
    if aggregation != "1 Hour":
        return sorted(list(counters), key=lambda x: (('EUTRANCELLFDD' in x or 'EUTRANCELLTDD' in x), x))
    else:
        counters_aggregations = get_aggregations(counters, table_type)
        updated_counters = set()

        for counter, agg in counters_aggregations.items():
            updated_counters.add(agg+'('+counter+') as "'+counter.split(".")[1]+'"')

        return sorted(list(updated_counters), key=lambda x: (('EUTRANCELLFDD' in x or 'EUTRANCELLTDD' in x), x))
        


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


#Modification

def check_if_kpi_specific_where_clause(where_clause, tbl_name, flexVal, VectorVal):
    """add special where clause like FLEX_FILTER=2 etc."""
    print(tbl_name)
    print(flexVal)
    print(VectorVal)
    if flexVal not in ['[all]', 'NA']:
        if '[' in flexVal:
            flxName = flexVal.replace(' ', '').replace(']', '').split('[')
            new_flxName = flxName[1] 

        f_list= ','.join("'{}'".format(i) for i in new_flxName.split(","))
        where_clause =  """{table}.FLEX_FILTERNAME in ({new_flxName})""".format(table=tbl_name, new_flxName=f_list)
    
    elif VectorVal not in ['[all]', 'NA', '[]']:
        
        counterName, counterIndexes = VectorVal.replace(' ', '').replace(']', '').split('[')
            
        indexElements = [int(e) for e in counterIndexes.split(',') if '-' not in e]
        indexes = indexElements
        rangeElements = [e for e in counterIndexes.split(',') if '-' in e]
        
        for rangeElement in rangeElements:
            indexElements += list(range(int(rangeElement.split('-')[0]), 1 + int(rangeElement.split('-')[1])))
        
        VectorVal = sorted(indexes)
        
        tmp = "("
        for i in VectorVal:
            tmp += str(i)+","
        tmp = tmp[:len(tmp)-1] + ")"
        
        VectorVal = tmp
                 
        where_clause = '''{table}.DCVECTOR_INDEX IN {val}'''.format(table=tbl_name, val=VectorVal)
    
    if where_clause != '(Empty)':
        return 'AND ' + where_clause
    return ''	

# Modification


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
            
            #Modifcication
            
            'flex_list': alarm_info['flex_value'],
            'vector_list': alarm_info["index_value"],
            
            
            
            'group_by': group_by,
            'past_present': time_flag_value,
            'kpi_specific_where_clause': check_if_kpi_specific_where_clause(alarm_info["kpi_specific_where_clause"],alarm_info['table_names'],alarm_info["flex_value"], alarm_info['index_value']),
            'join_keys': join_keys 
            
            #Modification
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
            
            #Modifcication
            
            'flex_list': alarm_info['flex_value'],
            'vector_list': alarm_info["index_value"],
            
            
            
            'group_by': group_by,
            'past_present': time_flag_value,
            'kpi_specific_where_clause': check_if_kpi_specific_where_clause(alarm_info["kpi_specific_where_clause"],alarm_info['table_names'],alarm_info["flex_value"], alarm_info['index_value']),
            'join_keys': join_keys 
            
            #Modification
        }]
    return details_per_query_list

def replace_node_collection(alarm_query_details_list):
    ''' the worker files need to recreate the node collection if it ever changes, so assign collection name and replace later'''
	
    for alarm_query_details_dict in alarm_query_details_list:
        alarm_query_details_dict["ne_list"] = '@node_collection'

    return alarm_query_details_list


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


def build_query(alarm, alarm_info):
    ''' for a single alarm - query builder main function - calls other functions to create single table kpi/counters query'''
    full_alarm_sql = []  
    past_present_dict = {'past':",'Past' as TIME_FLAG", 'present':",'Present' as TIME_FLAG"}
    union_alarms = ['pcd', 'pcd+cd', 'dynamic'] 

    #check logload status to get date times
    if 'UNION' not in alarm_info['formula']:
        full_alarm_sql.append(loaded_date_sql(alarm_info['eniqtablename'],alarm_info))
    else:
        full_alarm_sql.append(loaded_date_sql_complex(alarm_info))
    time_limits = get_time_limits(alarm_info)

    #for past/present alarms, create each query individually and then join with a UNION at the end
    if alarm_info['alarm_type'] in union_alarms:
        union_sql = []
        db_union_sql = []

        for time_flag, time_flag_value in past_present_dict.items():
            
            alarm_query_details = get_alarm_query_details(alarm_info, time_limits, time_flag, time_flag_value)
            #print "alarm_query_details ", alarm_query_details
            union_sql.append(build_table_query(alarm_query_details))
            #print "union_sql ", union_sql

            db_alarm_query_details = replace_node_collection(alarm_query_details)
            #print "db_alarm_query_details ", db_alarm_query_details
            db_union_sql.append(build_table_query(db_alarm_query_details))
            #print "db_union_sql ", db_union_sql

        alarm_query = ' UNION '.join(union_sql)
        alarm_query_for_scheduler = ' UNION '.join(db_union_sql)
       
    else:
        time_flag_value = ",'Present' as TIME_FLAG"
        time_flag = 'present'
        alarm_query_details = get_alarm_query_details(alarm_info, time_limits, time_flag, time_flag_value)
        #print "alarm_query_details ", alarm_query_details
        alarm_query = build_table_query(alarm_query_details)
        #print "alarm_query ", alarm_query

        db_alarm_query_details = replace_node_collection(alarm_query_details)
        #print "db_alarm_query_details ", db_alarm_query_details
        alarm_query_for_scheduler = build_table_query(db_alarm_query_details)
        #print "alarm_query_for_scheduler ", alarm_query_for_scheduler

    full_alarm_sql.append(alarm_query) 
    alarm_for_db.append(alarm_query_for_scheduler.replace('\n',''))

    return alarm, ''.join(full_alarm_sql)


def queries_from_alarm_list(alarms):
    ''' Builds a list of tuples 
        1st element is TableName and 2nd elemet is sql query
    '''

    return [build_query(alarm_name, alarm_info) for alarm_name, alarm_info in alarms.items()]


def add_datetime_calc_column(pm_table_name):
    """Adds a calculated column of DATETIME_ID to the pm table created from the SQL query

    Arguments:
        pm_table_name -- name of table to add calculated column to
    Returns:
        none
    """ 

    cols = Document.Data.Tables[pm_table_name].Columns
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

    alarm_lookback = alarm_details[alarm_name]["look_back_val"]

    if alarm_lookback >= 1440:
        lookback_period = str(int(alarm_lookback)/1440) # convert mins to day
        interval = 'day'
    else:
        lookback_period = alarm_lookback
        interval = 'mi'

    if alarm_table_ext=='_DAY':
        calc_col = "Date([DATE_ID])"
        date_column_name = 'Date'        
        lookback_col_date = '[DATE_ID]'
    else:
        calc_col = "Time([DATETIME_ID])"
        date_column_name="TIME"
        lookback_col_date = '[DATETIME_ID]'
 
    if alarm_type in ['cd','pcd','pcd+cd','trend'] and aggregation == '1 Day':
        add_date_time_id_column(pm_table_name)
        date_column_name="TIME"
        lookback_col = "If([TIME_FLAG]='Past',DateAdd('day',"+str(alarm_lookback)+",[DATETIME_ID]),[DATETIME_ID])"
        calc_col =  lookback_col

    if date_column_name not in pm_column_names:
        pm_columns.AddCalculatedColumn(date_column_name, calc_col)
    else:
        Document.Data.Tables[pm_table_name].Columns[date_column_name].As[CalculatedColumn]().Expression= calc_col

def add_date_time_id_column(pm_table_name):

    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]
    calc_col = "[DATE_ID]"
    date_time_id_column_name="DATETIME_ID"
    if date_time_id_column_name not in pm_column_names:
        pm_columns.AddCalculatedColumn(date_time_id_column_name, calc_col)
    else:
        Document.Data.Tables[pm_table_name].Columns[date_time_id_column_name].As[CalculatedColumn]().Expression= calc_col


def add_measure_value_column(pm_table_name,measure_count,measurename):

    measurename = measurename.replace(']', ')').replace('[', '(')
    table=Document.Data.Tables[pm_table_name]
    current_threshold_column_name="MEASUREVALUE_"+str(measure_count)
    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]

    if current_threshold_column_name not in pm_column_names:
        pm_columns.AddCalculatedColumn(current_threshold_column_name, "["+measurename.strip()+"]")
    else:
        Document.Data.Tables[pm_table_name].Columns[current_threshold_column_name].As[CalculatedColumn]().Expression= "["+measurename.strip()+"]"


def add_alarm_type_column(pm_table_name,alarm_type):

    alarm_type_name='Alarm Type'
    pm_columns = Document.Data.Tables[pm_table_name].Columns
    found,column=Document.Data.Tables[pm_table_name].Columns.TryGetValue(alarm_type_name)

    if found: 
        Document.Data.Tables[pm_table_name].Columns.Remove(alarm_type_name)  
        pm_columns.AddCalculatedColumn(alarm_type_name, "'"+alarm_type+"'")
    else:
        pm_columns.AddCalculatedColumn(alarm_type_name, "'"+alarm_type+"'")


def add_kpi_calc_column(pm_table_name,kpiname,kpiformula):

    kpiname = kpiname.replace(']', ')').replace('[', '(')
    pm_columns = Document.Data.Tables[pm_table_name].Columns
    pm_column_names = [column.Name for column in pm_columns]

    if kpiname not in pm_column_names:
        pm_columns.AddCalculatedColumn(kpiname.strip(), "Real("+kpiformula+")")


def hide_columns_in_table(data_table_name, table_plot, columns_to_hide):
    """gets a measure's table name with a given measure name

    Arguments:
        data_table_name -- String - data table used in table plot
        table_plot -- The table plot visualisation 
        columns_to_hide -- list - Columns to hide in table
    Returns:
        none
    """

    if columns_to_hide:
        columns = Document.Data.Tables[data_table_name].Columns

        for column in columns:
            if column.Name in columns_to_hide or ('(' in column.Name and column.Properties.ColumnType != DataColumnType.Calculated):
                table_plot.TableColumns.Remove(column)


def get_save_navigation_vis():
    """gets the save navigation from a page

    Arguments:
        none
    Returns:
        none
    """
    for page in Application.Document.Pages:
        for vis in page.Visuals:
            if vis.TypeId == VisualTypeIdentifiers.HtmlTextArea and vis.Title == 'Save Navigation':
                return vis
    return None


def show_data_table(data_table_name, columns_to_hide):
    ''' creates page when data table created from running query.'''

    page_titles_set = set()
    for page in Document.Pages:
        page_titles_set.add(page.Title)

    # Remove any existing result page, else create a new one
    if data_table_name in page_titles_set:
        for page in Document.Pages:
            if page.Title == data_table_name:
                return

    results_page = Document.Pages.AddNew(data_table_name)
    results_page.AutoConfigure()

    table_plot = results_page.Visuals.AddNew[TablePlot]()
    if Document.Data.Tables.Contains(data_table_name):
        table_plot.Data.DataTableReference = Document.Data.Tables[data_table_name]

    table_plot.AutoConfigure()
    table_plot.AutoAddNewColumns = True
    table_plot.Legend.Visible = False
    table_plot.Title = data_table_name

    hide_columns_in_table(data_table_name, table_plot, columns_to_hide)  
    navigation_vis = get_save_navigation_vis()

    layout = LayoutDefinition()
    layout.BeginStackedSection()
    layout.BeginSideBySideSection(90)
    layout.BeginStackedSection(80)
    layout.Add(table_plot.Visual, 100)
    layout.EndSection()
    layout.EndSection()
    layout.Add(results_page.Visuals.AddDuplicate(navigation_vis), 10)
    layout.EndSection()
    results_page.ApplyLayout(layout)

    Document.ActivePageReference = results_page
    Document.ActivePageReference.DetailsOnDemandPanel.Visible = False


def columns_not_to_remove(measure_columns):
    """gets a list of columns that should not be removed from the data table

    Arguments:
        measure_columns -- String List - columns used in measure/KPI not to be removed
    Returns:
        String List - columns not to remove from table
    """
    columns = ["CURRENT_DATESTAMP", "DATE_ID", "DATETIME_ID", "MEASUREVALUE_1", "MEASUREVALUE_2", "MEASUREVALUE_3", "MEASUREVALUE_4", "Threshold1", "Threshold2", "Alarm Type", "ALARM_CRITERIA"]
    columns.extend(measure_columns)

    return columns


def get_current_columns(table_name):
    """gets a list of columns from a data table

    Arguments:
        table_name -- String - Name of data table from whichh to get columns
    Returns:
        current_columns -- String List of column names
    """
    current_columns = []
    data_table = Document.Data.Tables[table_name]
    column_collection = data_table.Columns

    for column in column_collection:
        current_columns.append(column.Name)

    return current_columns

    
def get_spotfire_dt(table_name,filter_condition):
    """gets a list of columns from a data table

    Arguments:
        table_name -- String - Name of data table from whichh to get columns
        filter_condition -- condition to filter through table for
    Returns:
        source_data_table -- DataTableObject
        source_cur - cursor on a particular column
        index_set - filtered IndexSet
    """
    index_filter = Document.Data.Tables[table_name].Select(filter_condition)  
    index_set = index_filter.AsIndexSet()
    source_data_table = Document.Data.Tables[table_name]
    source_cur = create_cursor(source_data_table)
    
    return source_data_table,source_cur,index_set


def add_original_calc_columns(alarm_name):
    """Adds calculated columns from Alarm Formulas table
    If column expression is invalid, adds the expression as a string with Invalid Column

    Arguments:
        alarm_name -- String - Name of alarm from which to get columns
    Returns:
        None
    """

    alarm_columns = {}
    alarm_column_data_type ={}
    alarm_table = 'Alarm Formulas'
    filter_condition = """[AlarmName] = '{alarm_name}'""".format(alarm_name=alarm_name)
    alarm_formula_dt, alarm_formula_cursor, alarm_formula_index = get_spotfire_dt(alarm_table, filter_condition)

    cols_current = get_current_columns(alarm_name)

    # for given alarm name loop through tblAlarmFormulas and add to dict by column name and formula
    for selectedmeasure in alarm_formula_dt.GetRows(alarm_formula_index, Array[DataValueCursor](alarm_formula_cursor.values())):
        formula_column_name = alarm_formula_cursor['AlarmColumnName'].CurrentValue
        formula_value = alarm_formula_cursor['AlarmColumnFormula'].CurrentValue
        formula_data_type = alarm_formula_cursor['AlarmColumnDataType'].CurrentValue
        if formula_column_name not in cols_current:
            alarm_columns[formula_column_name] = formula_value
            alarm_column_data_type[formula_column_name] = formula_data_type
   
    # add calculated cols to table (same as alarm_name)
    alarm_table_cols = Document.Data.Tables[alarm_name].Columns

    # add all the col names first with datatype place holders. 
    # then add formulas (this avoids error with columns not added in order)
    for column_name, column_formula in alarm_columns.items():
        try:
            column_data_type = alarm_column_data_type[column_name]
            placeholder_expression = PLACEHOLDER_EXPRESSIONS[column_data_type]
        except Exception as e:
            placeholder_expression = "'0'"
        alarm_table_cols.AddCalculatedColumn(column_name, placeholder_expression)

    for column_name, column_formula in alarm_columns.items():
        try:
            calc_col = Document.Data.Tables[alarm_name].Columns[column_name].As[CalculatedColumn]()
            calc_col.Expression = column_formula
        except Exception as e:
            calc_col.Expression = "'Invalid column: "+column_formula+"'"
            pass


def run_query(table_name,query):
    print("START")
    print(query)
    print("END")
    try:
        db_settings = DatabaseDataSourceSettings("System.Data.Odbc", connString, query)
        ds = DatabaseDataSource(db_settings)

        if Document.Data.Tables.Contains(table_name):

            dataTable = Document.Data.Tables[table_name]
            dataTable.ReplaceData(ds)
        else:
            Document.Data.Tables.Add(table_name, ds)
    except Exception as e:
       print("Exception: ", e)


def write_query_to_doc_prop():
    ''' write the alarm query to a document property'''
    alarm_query = ''.join(alarm_for_db)

    Document.Properties["CurrentSQLQuery"] = alarm_query


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


def main():
    ''' main function to create tables from queries'''
    try:
        for alarm_name, query in alarm_queries: 
            alarm_type=alarm_details[alarm_name]['alarm_type']
            aggregation=alarm_details[alarm_name]['aggregation']
            alarm_table_ext=alarm_details[alarm_name]['alarm_table_ext']
        
            source_table_name = alarm_name
            alarm_table_name=alarm_name

            run_query(source_table_name,query)
            Document.Data.Tables[alarm_table_name].Refresh()
            columns_to_hide = []
            if alarm_name in kpis:

                if aggregation == "1 Hour":
                    add_datetime_calc_column(alarm_table_name)
           
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
                    
                    columns_to_hide.append(measurename)
                add_alarm_type_column(alarm_table_name, alarm_type)
                if Document.Properties["IsEdit"] == "Edit":
                    add_original_calc_columns(alarm_name)
                            
            write_query_to_doc_prop()    
            show_data_table(alarm_table_name, columns_to_hide)


    except Exception as e:
        notify.AddWarningNotification("Exception","Error in fetching data",str(e))
        print("Exception: ", e)

            
# create collection cursor, used in for getting nodes in other functions
node_collections_data_table_name = 'NodeCollection'
node_collections_data_table = Document.Data.Tables[node_collections_data_table_name]
node_collection_cur = create_cursor(node_collections_data_table)

# create alarm queries and create tables
alarm_details = get_alarm_details()


if Document.Properties["ValidationError"] == "":
    alarm_queries = queries_from_alarm_list(alarm_details)
    print alarm_queries
    main()
    #trigger apply template script
    if Document.Properties["IsEdit"] == "Create":
        Document.Properties["applyTemplate"] = DateTime.UtcNow