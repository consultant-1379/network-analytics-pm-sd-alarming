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
# Name    : SingleMeasureSelect.py
# Date    : 09/12/2020
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarming
#

from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Application import PanelTypeIdentifiers
from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters

src_table = Document.Data.Tables["Measure Mapping"]

def get_selected_measure():
    """
    Gets selected measure from measures filter
    Sets error message if more than one measure is selected
    """
    filt=Document.FilteringSchemes[1][src_table][src_table.Columns["Measure"]].As[ListBoxFilter]()
    if len(filt.SelectedValues) == 1:
        for value in filt.SelectedValues:
            Document.Properties["ErrorLabelMultipleMeasureSelected"]=""
            Document.Properties["SelectedMeasureName"] = str(value)
    else:
        Document.Properties["SelectedMeasureName"]=""
        Document.Properties["ErrorLabelMultipleMeasureSelected"]="Please select only one measure to proceed"

get_selected_measure()
