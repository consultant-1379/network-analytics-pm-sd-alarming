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
# Name    : ApplyTemplate.py
# Date    : 27/11/2020
# Revision: 1.0
# Purpose : Applies template to data table based on alarm type
#
# Usage   : PM Alarms
#
from System import Array
from Spotfire.Dxp.Data import CalculatedColumn
from Spotfire.Dxp.Data import DataValueCursor
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService

notify = Application.GetService[NotificationService]()

templates_dict={
    'cd':'CdTemplate', 
    'pcd':'PcdTemplate', 
    'dynamic':'DynamicTemplate', 
    'trend':'TrendTemplate', 
    'pcd+cd':'CdPcdTemplate',
    'threshold':'ThresholdTemplate',
    'cdt': 'CdtTemplate'
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
        expression=col_properties.GetProperty("Expression")
        if str(column_type) == 'Calculated':
            template_calculated_cols.append(col_name)
    
    return template_calculated_cols


def apply_template(alarm_name,alarm_type):
    """Applies corresponding alarm template and its' intermediate calculated columns to the data table - Pm Alarming

    Arguments:
        alarm_name {string} -- 'AlarmName' from Alarm Definitions table
        alarm_type {string} -- 'AlarmType' from Alarm Definitions table
    """
    try:
        template_table_name=templates_dict.get(alarm_type)
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
                
                table_cols.AddCalculatedColumn(col, placeholder_expression)
     
        for col in template_calculated_columns:
            col_name=str(col)
            template_column = template_table.Columns.Item[col_name]
            col_properties= template_column.Properties
            column_type=col_properties.GetProperty("ColumnType")
            if str(column_type) == 'Calculated':
                expression=col_properties.GetProperty("Expression")
                Document.Data.Tables[complete_table_name].Columns[col_name].As[CalculatedColumn]().Expression= expression
                
    except Exception as e:
        notify.AddWarningNotification("Exception","Data Table Does Not Exists",str(e))


      
def get_selected_alarms():
    """Retrieves user selected Alarm Rules form Alarm Definitions table - Pm Alarming"""
    

    alarm_name = Document.Properties['AlarmName']
    alarm_type = Document.Properties["AlarmType"]
    apply_template(alarm_name,alarm_type)

        
get_selected_alarms()
