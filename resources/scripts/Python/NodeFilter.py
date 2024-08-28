from System import Array, Object
from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters
from Spotfire.Dxp.Application import PanelTypeIdentifiers
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Data import *

def create_cursor(eTable):
    """Create cursors for a given table, these are used to loop through columns"""

    cursList = []
    colList = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cusrDict = dict(zip(colList, cursList))
    return cusrDict

imp = ['CollectionName']
for p in Document.Pages:
    if(p.Title =="Alarm Rules Manager" or p.Title =="Alarm Rules Import Manager"):
        for panel in p.Panels:
            if panel.TypeId == PanelTypeIdentifiers.FilterPanel:
                for group in panel.TableGroups:
                    if group.Name == "NodeCollection":
                            for fh in group.FilterHandles:
                                if fh.FilterReference.Name in imp:
                                    fh.FilterReference.TypeId=FilterTypeIdentifiers.ListBoxFilter
                                    thelistboxFilter = fh.FilterReference.As[filters.ListBoxFilter]()
                                    thelistboxFilter.IncludeAllValues = False
                                    try:
                                        aProp= Document.Properties["NeCollection"]
                                        thelistboxFilter.SetSelection(aProp)
                                    except:
                                        print "Todo: Do something with this"

srcTable = Document.Data.Tables["NodeCollection"]
DataFilteringSelection=Document.ActiveFilteringSelectionReference
filt1=Document.FilteringSchemes[1][srcTable][srcTable.Columns["CollectionName"]].As[ListBoxFilter]()
filt1.IncludeAllValues = False

src_table_cursor = create_cursor(srcTable)
ne_collection = Document.Properties['NeCollection']
src_table_filter = srcTable.Select("[CollectionName]= '" + ne_collection + "'")
for measure in srcTable.GetRows(src_table_filter.AsIndexSet(), Array[DataValueCursor](src_table_cursor.values())):
    node_type = src_table_cursor['NodeType'].CurrentValue
    Document.Properties['NodeType'] = node_type
try:
    aProp= Document.Properties["NeCollection"]
    filt1.SetSelection(aProp)
except:
    print 'test'