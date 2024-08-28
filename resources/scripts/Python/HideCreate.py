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
# Name    : HideCreate.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from System.Collections.Generic import List
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Data import*
import Spotfire.Dxp.Application.Filters as filters
import Spotfire.Dxp.Application.Filters.CheckBoxFilter


Document.Properties["selectedNodes"] = ""
Document.Properties["SelectedCollectionToModify"] = None
Document.Properties['CollectionName'] = "Collection_"
Document.Properties["WildCardExpression"] = ""
Document.Properties["NodeList"] = ""
selectedNodeDataTableName = 'SelectedNodes'
nodeCollectionsDataTableName = 'NodeCollection'
Document.Properties['DynamicCollection'] = 'OFF'
slelectedCollectionDP = Document.Properties["SelectedCollectionToModify"]

MyTable=Document.Data.Tables["NodeList"]


def populateSelectNodesDT():
    #if Document.Data.Tables.Contains(nodeCollectionsDataTableName):
    nodeCollectionsDataTable = Document.Data.Tables[nodeCollectionsDataTableName]
    activeCollection = nodeCollectionsDataTable.Select("[CollectionName] !='" + selectedCollectionToModify() + "'")
    print activeCollection.IncludedRowCount
    dataSource = DataTableDataSource(nodeCollectionsDataTable)
    if Document.Data.Tables.Contains(selectedNodeDataTableName):
        # Document.Data.Tables.Remove(selectedNodeDataTableName)
        selectedNodeDataTable = Document.Data.Tables[selectedNodeDataTableName]
        selectedNodeDataTable.ReplaceData(dataSource)
        selectedNodeDataTable.RemoveRows(activeCollection)


def checkAlarmForCollection(NE_Collection):
    activeCollection = alarmDefinitionsDataTable.Select("[NECollection] ='" + NE_Collection + "'")
    Document.Properties["DeleteCollectionsError"] = activeCollection.IsEmpty
    
    return activeCollection.IsEmpty


def selectedCollectionToModify():
    dataTable = Document.Data.Tables["NodeCollection"]
    cursor = DataValueCursor.CreateFormatted(dataTable.Columns["CollectionName"])
    cursor1 = DataValueCursor.CreateFormatted(dataTable.Columns["CollectionName"])
    markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)

    
    if markings.IncludedRowCount != 1:
        Document.Properties["ErrorMessage"] = "To Edit a Collection, select one collection"
        return None	
    else:
        Document.Properties["ErrorMessage"] = ""	
        markedata = List [str]();
        for row in dataTable.GetRows(markings.AsIndexSet(),cursor):
	    #rowIndex = row.Index ##un-comment if you want to fetch the row index into some defined condition
	    value1 = cursor.CurrentValue
	    if value1 <> str.Empty:
		    markedata.Add(value1)
            return markedata[0]

#print selectedCollectionToModify()
if selectedCollectionToModify() != None:
    Document.Properties['CollectionName'] = selectedCollectionToModify() 
    populateSelectNodesDT()   
    for page in Document.Pages:
        if (page.Title == 'Node Collection  Manager'):
            Document.Properties['SearchNodes'] = ""
            Document.Properties['searchSelectedNodes'] = ""
            dataTable = Document.Data.Tables["NodeCollection"]
            MyTable.RemoveRows(RowSelection(IndexSet(MyTable.RowCount,True)))
            cursorNodeType = DataValueCursor.CreateFormatted(dataTable.Columns["NodeType"])
            cursorSystemArea = DataValueCursor.CreateFormatted(dataTable.Columns["SystemArea"])
            cursorCollectionType = DataValueCursor.CreateFormatted(dataTable.Columns["CollectionType"])
            cursorEniqName = DataValueCursor.CreateFormatted(dataTable.Columns["EniqName"])
            cursorExpression = DataValueCursor.CreateFormatted(dataTable.Columns["WildCardDefinition"])
            markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)
        
            Document.ActivePageReference=page
            Document.Properties['ConnectionError'] =  ""
            Document.Properties['SystemAreaErrorMsg'] = "Collection Name, System Area, Node Type and, Eniq Data Source will be uneditable"
            Document.Properties['NodeConfiguration'] = "EDIT"
            Document.Properties['CreateModify'] = "Modify Collection"
            

            for row in dataTable.GetRows(markings.AsIndexSet(), cursorNodeType, cursorSystemArea,cursorCollectionType,cursorExpression,cursorEniqName):
                Document.Properties['NodeType'] = cursorNodeType.CurrentValue
                Document.Properties['SystemArea']= cursorSystemArea.CurrentValue
                nodeType = Document.Properties['NodeType']
                Document.Properties['ENIQDataSourcesDropDown'] = cursorEniqName.CurrentValue
                print cursorEniqName.CurrentValue
                systemArea = Document.Properties['SystemArea']
                CollectionName = Document.Properties['CollectionName']
                dataSourceName = Document.Properties['ENIQDataSourcesDropDown']
                FilterSelection = Document.Data.Filterings["DynamicCollectionFilters"]
                collectionFilter = Document.FilteringSchemes[FilterSelection][Document.Data.Tables["DynamicCollectionOperation"]]["DynamicCollectionOperation"].As[filters.CheckBoxFilter]()
				
                if cursorCollectionType.CurrentValue == 'Dynamic':
					Document.Properties['DynamicCollection'] = 'ON'
					Document.Properties["WildCardExpression"] = cursorExpression.CurrentValue
					wildcardExpression = Document.Properties["WildCardExpression"]
					SelectedNodeTable = Document.Data.Tables['SelectedNodes']
					SelectedNodeTable.RemoveRows(RowSelection(IndexSet(SelectedNodeTable.RowCount,True)))
					for CheckBoxValue in collectionFilter.Values:
						collectionFilter.Check(CheckBoxValue)
                else:
					for CheckBoxValue in collectionFilter.Values:
						collectionFilter.Uncheck(CheckBoxValue)
					Document.Properties['WildcardExpression'] = ""
					
            



