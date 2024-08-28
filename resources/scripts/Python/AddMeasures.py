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
# Name    : AddMeasures.py
# Date    : 24/11/2020
# Revision: 1.0
# Purpose : Adds up to four user-selected KPIs to the Alarm definition create UI
#
# Usage   : PM Alarming
#

from System.IO import MemoryStream, StreamWriter, SeekOrigin
from System.Collections.Generic import List
from System import Array
from Spotfire.Dxp.Application.Filters import ListBoxFilter
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import TextDataReaderSettings, TextFileDataSource
import re

data_table_name = 'Measure Mapping'
data_table = Document.Data.Tables[data_table_name]

def split_measures():  
    '''splits measures string into list'''  
    current_measures = Document.Properties["SelectedMeasureList"]
    if current_measures != "":
        measures_list = current_measures.split(';')
    else:
        measures_list = []
    return measures_list
    
def add_measures():
    '''adds selected measure to measure list'''
    filt=Document.FilteringSchemes[1][data_table][data_table.Columns["Measure"]].As[ListBoxFilter]()
    if len(filt.SelectedValues) == 1 and num_selected_measures < 4:
        for value in filt.SelectedValues:
            if value not in current_measures_list:
                current_measures_list.append(value)
            else:
                Document.Properties["ErrorLabelMultipleMeasureSelected"] = "This measure is already selected"
                

def display_measures():
    '''displays selected measures on UI'''
    filt=Document.FilteringSchemes[1][data_table][data_table.Columns["Measure"]].As[ListBoxFilter]()
    num_of_KPIs = 0

    for value in current_measures_list:
        num_of_KPIs += 1 
        if num_of_KPIs == 1:
            Document.Properties["SelectedMeasureList"] = value
            Document.Properties["SelectedKPI1"] = value
        elif num_of_KPIs <= 4:
            Document.Properties["SelectedMeasureList"] += ";" + value
            Document.Properties["SelectedKPI" + str(num_of_KPIs)] = value
        

Document.Properties["SelectedKPI1"] = ""
Document.Properties["SelectedKPI2"] = ""
Document.Properties["SelectedKPI3"] = ""
Document.Properties["SelectedKPI4"] = ""

current_measures_list = split_measures()
num_selected_measures = len(current_measures_list)
if num_selected_measures < 4:
    add_measures()
else:
    Document.Properties["ErrorLabelMultipleMeasureSelected"] = "Max number of measures selected"

display_measures()