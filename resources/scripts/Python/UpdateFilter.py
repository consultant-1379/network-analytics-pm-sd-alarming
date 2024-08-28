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
# Name    : UpdateFilter.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters
from Spotfire.Dxp.Application import PanelTypeIdentifiers


imp = ['MeasureType']
for p in Document.Pages:
    if(p.Title =="Alarm Rules Editor"):
        for panel in p.Panels:
                if panel.TypeId == PanelTypeIdentifiers.FilterPanel:
                    for group in panel.TableGroups:
                        if group.Name == "Measure Mapping":
                            #group.AddNewSubGroup("Subgroup")
                            # for subGroup in group.SubGroups:
                            #     print subGroup
                                for fh in group.FilterHandles:
                                    if fh.FilterReference.Name in imp:
                                        
                                        fh.FilterReference.TypeId=FilterTypeIdentifiers.CheckBoxFilter
                                        checkboxFilter = fh.FilterReference.As[filters.CheckBoxFilter]()
                                        checkboxFilter.Check("KPI")
                                        checkboxFilter.Check("Counter")
                                        aProp= Document.Properties["MeasureType"]
                                        checkboxFilter.UncheckAll()
                                        checkboxFilter.Check(aProp)
                                        
                                         

        
