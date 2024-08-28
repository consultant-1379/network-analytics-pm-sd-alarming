from Spotfire.Dxp.Application.Visuals.ConditionalColoring import *
from Spotfire.Dxp.Application.Visuals import TablePlot, VisualTypeIdentifiers, LineChart, CrossTablePlot, HtmlTextArea
import re
from Spotfire.Dxp.Data import *


if Document.Properties['NodeConfiguration'] != "EDIT":
	NodeListTable = Document.Data.Tables['NodeList']
	SelectedNodeTable = Document.Data.Tables['SelectedNodes']
	SelectedNodeTable.RemoveRows(RowSelection(IndexSet(SelectedNodeTable.RowCount,True)))
	NodeListTable.RemoveRows(RowSelection(IndexSet(NodeListTable.RowCount,True)))



for page in Application.Document.Pages:
    if Document.ActivePageReference == page:
        for vis in page.Visuals:
            if vis.TypeId == VisualTypeIdentifiers.HtmlTextArea and vis.Title == 'Text Area':
                source_html = vis.As[HtmlTextArea]().HtmlContent
                deshtml=source_html
                if Document.Properties['Dynamiccollection'] == 'ON':
					deshtml = re.sub('<DIV style="VISIBILITY: (visible|hidden)"><SPAN id=add><SpotfireControl id="ada84dd5f01f48798a238912b00bc12e" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV style="VISIBILITY: hidden"><SPAN id=add><SpotfireControl id="ada84dd5f01f48798a238912b00bc12e" /></SPAN></DIV> <DIV style="VISIBILITY: visible">',deshtml)
					deshtml = re.sub('<DIV align=center style="VISIBILITY: (visible|hidden)"><SpotfireControl id="e0a5aa7077cb47f5b4f9e01b24c402b3" /></DIV> <DIV align=center class=smthng style="VISIBILITY: (visible|hidden)">','<DIV align=center style="VISIBILITY: hidden"><SpotfireControl id="e0a5aa7077cb47f5b4f9e01b24c402b3" /></DIV> <DIV align=center class=smthng style="VISIBILITY: visible">',deshtml)
                else:
					deshtml = re.sub('<DIV style="VISIBILITY: (visible|hidden)"><SPAN id=add><SpotfireControl id="ada84dd5f01f48798a238912b00bc12e" /></SPAN></DIV> <DIV style="VISIBILITY: (visible|hidden)">','<DIV style="VISIBILITY: visible"><SPAN id=add><SpotfireControl id="ada84dd5f01f48798a238912b00bc12e" /></SPAN></DIV> <DIV style="VISIBILITY: hidden">',deshtml)
					deshtml = re.sub('<DIV align=center style="VISIBILITY: (visible|hidden)"><SpotfireControl id="e0a5aa7077cb47f5b4f9e01b24c402b3" /></DIV> <DIV align=center class=smthng style="VISIBILITY: (visible|hidden)">','<DIV align=center style="VISIBILITY: visible"><SpotfireControl id="e0a5aa7077cb47f5b4f9e01b24c402b3" /></DIV> <DIV align=center class=smthng style="VISIBILITY: hidden">',deshtml)                
                vis.As[HtmlTextArea]().HtmlContent = deshtml 
