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
# Name    : ValidateAggregation.py
# Date    : 02/02/2021
# Revision: 1.0
# Purpose : Adjusts lookback/data range dropdowns depending on aggregation dropdown
#
# Usage   : PM Alarming
#
aggregation = Document.Properties["Aggregation"]
lookback_units = Document.Properties["LookbackUnit"]
datarange_units = Document.Properties["DataRangeUnit"]

agg_periods = {
    "None": 0,
    "ROP": 15,
    "1 Hour": 60,
    "1 Day": 1440
}

units = {
    "ROP": 15,
    "HOUR": 60,
    "DAY": 1440
}

def change_dropdown_value(dropdown_name):
    """Changes the value of the lookback/data range dropdowns on the UI based on aggregation

    Arguments:
        dropdown_name - string name of dropdown document property
    """
    for key, value in units.items():
        val = agg_periods[aggregation]
        if val == value:
            Document.Properties[dropdown_name] = key


if units[lookback_units] < agg_periods[aggregation]:
    change_dropdown_value("LookbackUnit")
if units[datarange_units] < agg_periods[aggregation]:
   change_dropdown_value("DataRangeUnit")