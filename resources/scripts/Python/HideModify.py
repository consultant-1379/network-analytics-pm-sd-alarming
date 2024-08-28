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
# Name    : HideModify.py
# Date    : 24/06/2020
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarms
#


from Spotfire.Dxp.Data import RowSelection, IndexSet


from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Data import*
import Spotfire.Dxp.Application.Filters as filters
import Spotfire.Dxp.Application.Filters.CheckBoxFilter 

Document.Properties["selectedNodes"] = ""
Document.Properties["SelectedCollectionToModify"] = None
Document.Properties['CollectionName'] = "Collection_"
Document.Properties["WildCardExpression"] = ""
MyTable=Document.Data.Tables["NodeList"]


selectedNodeDataTableName = 'SelectedNodes'
nodeCollectionsDataTableName = 'NodeCollection'
slelectedCollectionDP = Document.Properties["SelectedCollectionToModify"]
if(slelectedCollectionDP == None):
    slelectedCollectionDP = ''


def populateSelectNodesDT():
    #if Document.Data.Tables.Contains(nodeCollectionsDataTableName):
    nodeCollectionsDataTable = Document.Data.Tables[nodeCollectionsDataTableName]
    activeCollection = nodeCollectionsDataTable.Select("[CollectionName] !='" + slelectedCollectionDP + "'")
    print activeCollection.IncludedRowCount
    dataSource = DataTableDataSource(nodeCollectionsDataTable)
    if Document.Data.Tables.Contains(selectedNodeDataTableName):
        #Document.Data.Tables.Remove(selectedNodeDataTableName)
        selectedNodeDataTable = Document.Data.Tables[selectedNodeDataTableName]
        selectedNodeDataTable.ReplaceData(dataSource)
        selectedNodeDataTable.RemoveRows(activeCollection)


populateSelectNodesDT()
for page in Document.Pages:
    if (page.Title == 'Node Collection  Manager'):
        Document.ActivePageReference=page
        Document.Properties['SystemArea'] = ""
        Document.Properties['NodeType'] = ""
        Document.Properties['ConnectionError'] = ""
        Document.Properties['SystemAreaErrorMsg'] = ""
        Document.Properties['SearchNodes'] = ""
        Document.Properties['searchSelectedNodes'] = ""
        Document.Properties['DynamicCollection'] = "OFF"
        MyTable.RemoveRows(RowSelection(IndexSet(MyTable.RowCount,True)))
        Document.Properties['NodeConfiguration'] = "Create"
        Document.Properties['CreateModify'] = "Create Collection"
        FilterSelection = Document.Data.Filterings["DynamicCollectionFilters"]
        dynCollectionFilter = Document.FilteringSchemes[FilterSelection][Document.Data.Tables["DynamicCollectionOperation"]]["DynamicCollectionOperation"].As[filters.CheckBoxFilter]()
	
        for CheckBoxValue in dynCollectionFilter.Values:
			dynCollectionFilter.Uncheck(CheckBoxValue)
        Document.Properties['WildcardExpression'] = ""

