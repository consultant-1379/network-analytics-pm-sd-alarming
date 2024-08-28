# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2019 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : CreateTable.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Application import PanelTypeIdentifiers
from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters

srcTable = Document.Data.Tables["Measure Mapping"]

def getSelectedMeasure():
    filt=Document.FilteringSchemes[0][srcTable][srcTable.Columns["Measure"]].As[ListBoxFilter]()
    print len(filt.SelectedValues)
    if len(filt.SelectedValues) == 1:
        for value in filt.SelectedValues:
            Document.Properties["ErrorLabelMultipleMeasureSelected"]=""
            Document.Properties["SelectedMeasureName"] = str(value)
    else:
        print len(filt.SelectedValues)
        Document.Properties["SelectedMeasureName"]=""
        Document.Properties["ErrorLabelMultipleMeasureSelected"]="Please select only one measure to proceed"

getSelectedMeasure()