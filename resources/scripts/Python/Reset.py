# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : Reset.py
# Date    : 17/01/2021
# Revision: 3.0
# Purpose : 
#
# Usage   : PM Alarms
#
from Spotfire.Dxp.Data import DataFilteringSelection, RelatedRowsPropagation                                

from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters

toTable=Document.Data.Tables["Measure Mapping"]
fromTable=Document.Data.Tables["NodeCollection"]
DataFilteringSelection=Document.ActiveFilteringSelectionReference
srcTable = Document.Data.Tables["Measure Mapping"]
if Document.Properties["SingleOrCollection"]=="Collection":
    filt1=Document.FilteringSchemes[1][srcTable][srcTable.Columns["Node Type"]].As[CheckBoxFilter]()
    filt1.Reset()
    DataFilteringSelection.SetRelationPropagationBehavior(toTable,fromTable,RelatedRowsPropagation.OnlyMatching)
else:
    DataFilteringSelection.SetRelationPropagationBehavior(toTable,fromTable,RelatedRowsPropagation.Ignore)
    filt1=Document.FilteringSchemes[1][srcTable][srcTable.Columns["Node Type"]].As[CheckBoxFilter]()
    #filt2=Document.FilteringSchemes[1][srcTable][srcTable.Columns["DataSourceName"]].As[CheckBoxFilter]()
    filt1.UncheckAll()
    #filt2.UncheckAll()
    filt1.Check(Document.Properties["NodeType"])
    #filt2.Check(Document.Properties["ENIQDataSourcesDropDown"])

filt=Document.FilteringSchemes[1][srcTable][srcTable.Columns["Measure Type"]].As[CheckBoxFilter]()
if Document.Properties["MeasureType"]=="Counter":
    filt.Check("Counter")
    filt.Uncheck("KPI")
    filt.Uncheck("RI")
    filt.Uncheck("Custom KPI")
elif Document.Properties["MeasureType"]=="KPI":
    filt.Check("KPI")
    filt.Uncheck("Counter")
    filt.Uncheck("RI")
    filt.Uncheck("Custom KPI")
elif Document.Properties["MeasureType"]=="Custom KPI":
    filt.Check("Custom KPI")
    filt.Uncheck("KPI")
    filt.Uncheck("Counter")
    filt.Uncheck("RI")
elif Document.Properties["MeasureType"]=="RI":
    filt.Check("RI")
    filt.Uncheck("Counter")
    filt.Uncheck("KPI")
    filt.Uncheck("Custom KPI")
elif Document.Properties["MeasureType"]=="None":
    filt.Uncheck("Counter")
    filt.Uncheck("KPI")
    filt.Uncheck("RI")
    filt.Uncheck("Custom KPI")

