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
# Name    : changeListboxHeight.py
# Date    : 29/4/2020
# Revision: 1.0
# Purpose : 
#
# Usage   : Change listbox height in collection manager (used only once, does not need to be ran multiple times)
#
from Spotfire.Dxp.Application.Filters import ListBoxFilter
# getting value from property (property type is integer)

dt = Document.Data.Tables["SelectedNodes"]
listFilter = Document.FilteringSchemes.DefaultFilteringSchemeReference[dt]['NodeName'].As[ListBoxFilter]()

listFilter.Height = 40

dt = Document.Data.Tables["NodeList"]
listFilter = Document.FilteringSchemes.DefaultFilteringSchemeReference[dt]['node'].As[ListBoxFilter]()

listFilter.Height = 40