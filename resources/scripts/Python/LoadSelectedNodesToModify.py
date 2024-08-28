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
# Name    : LoadSelectedNodesToModify.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : 
#
# Usage   : PM Alarms
#

from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Data import*


selectedNodeDataTableName = 'SelectedNodes'
nodeCollectionsDataTableName = 'NodeCollection'
selectedCollectionDP = Document.Properties["SelectedCollectionToModify"]
alarmDefinitionsDataTableName = 'Alarm Definitions'
alarmDefinitionsDataTable = Document.Data.Tables[alarmDefinitionsDataTableName]


if(selectedCollectionDP==None):
    selectedCollectionDP=''


def populateSelectNodesDT():
    #if Document.Data.Tables.Contains(nodeCollectionsDataTableName):
    nodeCollectionsDataTable = Document.Data.Tables[nodeCollectionsDataTableName]
    activeCollection = nodeCollectionsDataTable.Select("[CollectionName] !='" + selectedCollectionDP + "'")
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

print selectedCollectionDP
print checkAlarmForCollection(selectedCollectionDP)
checkAlarmForCollection(selectedCollectionDP)
populateSelectNodesDT()
Document.Properties['CollectionName'] = selectedCollectionDP
