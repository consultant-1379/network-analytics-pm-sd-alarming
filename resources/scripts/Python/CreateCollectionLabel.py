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
# Name    : CreateCollectionLabel.py
# Date    : 02/09/2019
# Revision: 1.0
# Purpose : creating collection label for nodes
#
# Usage   : PM Alarms
#

import clr
import System
from System.Data import DataSet, DataTable, XmlReadMode
from Spotfire.Dxp.Data import DataType, DataTableSaveSettings
from System.IO import StringReader, StreamReader, StreamWriter, MemoryStream, SeekOrigin

clr.AddReference('System.Data')

# getting the user defined collection name
Collection_Name = Document.Properties['CollectionName']
collection_list = []
collection_list.add(Collection_Name)

dataSet = DataSet()
dataTable = DataTable("Collection")
dataTable.Columns.Add("CollectionName", System.String)
dataSet.Tables.Add(dataTable)

for row in valData:
   dt["CollectionName"] = Collection_Name
   dataTable.Rows.Add(dt)


dataTable.Refresh()
