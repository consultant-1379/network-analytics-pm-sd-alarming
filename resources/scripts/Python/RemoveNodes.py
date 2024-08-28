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

# Name    : RemoveNodes.py

# Date    : 21/09/2021

# Revision: 1.0

# Purpose : 

#

# Usage   : PM Explorer

#



from Spotfire.Dxp.Data import IndexSet, RowSelection, DataValueCursor




selected_node_table = Document.Data.Tables['SelectedNodes']



data_filtering_selection = Document.Data.Filterings["Filtering scheme"]

filtering_scheme = Document.FilteringSchemes[data_filtering_selection]

filter_collection = filtering_scheme[selected_node_table]

filtered_rows = filter_collection.FilteredRows




cursor = DataValueCursor.CreateFormatted(selected_node_table.Columns["NodeName"])

rows_to_remove=IndexSet(selected_node_table.RowCount,False)



def remove_rows():

    """Loops through the selected rows in the SelectedNodes table and adds

       them to an IndexSet which then gets removed from the SelectedNodes table

    """

    for row in selected_node_table.GetRows(filtered_rows,cursor):

        rows_to_remove.AddIndex(row.Index)

    selected_node_table.RemoveRows(RowSelection(rows_to_remove))




remove_rows()