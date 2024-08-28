# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2021 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : ClearAlarmRulesEditor.py
# Date    : 04/01/2021
# Revision: 2.0
# Purpose : 
#
# Usage   : PM Alarming
#

Document.Properties['AlarmName'] = ''
Document.Properties['SelectedMeasureList'] = ''
Document.Properties['ProbableCause'] = ''
Document.Properties['SpecificProblem'] = ''
Document.Properties['isEdit'] = 'Create'
Document.Properties['SingleOrCollection'] = 'Collection'
Document.Properties['SystemArea'] = 'None'
Document.Properties['NodeType'] = 'None'
Document.Properties['MeasureType'] = 'None'
Document.Properties['ErrorLabelMultipleMeasureSelected'] = ''
Document.Properties['NECollection'] = 'None'
Document.Properties['ValidationError'] = ''
Document.Properties['SelectedKPI1'] = ''
Document.Properties['SelectedKPI2'] = ''
Document.Properties['SelectedKPI3'] = ''
Document.Properties['SelectedKPI4'] = ''
Document.Properties["ErrorLabelMultipleNodeSelected"]=''
Document.Properties["Aggregation"] = 'None'
Document.Properties['LookbackPeriod'] = '1'
Document.Properties['LookbackUnits'] = 'ROP'
Document.Properties['DataRangePeriod'] = '1'
Document.Properties['DataRangeUnits'] = 'ROP'

Document.Properties['ExportMessage'] = ''

for page in Document.Pages:
	if (page.Title == 'Alarm  Rules Manager'):
		Document.ActivePageReference=page
        Document.Properties["Action"] = "Create"