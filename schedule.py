#!/usr/bin/env python2
# coding: utf-8

### schedule setup script
### writen by Hodjat Moradi  Email: hojm@equinor.com
# 07-05-2019, hojm, The startup_date function is movedto functions.py. the well opening and phases startup times are dumped as 'drilling_schedule.csv'
# 13-05-2019, hojm, Phase 1 functunality added so script reads and generates the phase1 only schedule
# 16-07-2019, hojm, almost all of the kywords in the schedule added.
# 07-10-2019, hojm, phase1 startup uncertainty removed. The start of prediction added as a new refrence point.
# 09-10-2019, hojm, phase1 only functionality is fixed for drilling schedule and group control
# changing the order might be needed.

import sys, os, shutil
import datetime as dt
import pandas as pd
import numpy as np
from functions import *
#from glob import glob

draft_file = './draft_schedule.sch'
if os.path.isfile(draft_file):
    os.remove(draft_file)

schedule_input   = sys.argv[1]
master_schedule  = sys.argv[2]
parameters = sys.argv[3]
ffORph1 = sys.argv[4]
ref_ph1start = sys.argv[5]
ref_predStart = sys.argv[6] #change
ref_ph2start = sys.argv[7] #change



#schedule_input = './schedule_input_dev.xlsx'
#master_schedule = './SCH_20171025_NPS.SCH'
#parameters = './parameters.txt'
#ffORph1 = 'ff'
# ref_ph1start = '277'
# ref_predStart = '292'#change
# ref_ph2start = '1369'

 
target_fn = 'SCHEDULE.OK'
if os.path.isfile(target_fn):
    os.remove(target_fn)

predStart, phase2start = ref_dates(ref_predStart, ref_ph2start, parameters) #change

schedule = matrix[doe(parameters, 'schedule')]
gefac = matrix[doe(parameters, 'gefac')]
gconprod = matrix[doe(parameters, 'gconprod')]
injectionRate = matrix[doe(parameters, 'INJECTION_RATE')]
ramp_up = matrix[doe(parameters, 'RAMP_UP')]

#### WLIST ################################################################################
excelColumns = 'E:T'
data = pd.read_excel(schedule_input, sheet_name='WLIST', skiprows=2,  usecols=excelColumns).dropna(axis=0, how='all')
data.iloc[:,4:] = "'" + data.iloc[:,4:] + "'"

key_col = ['date', 'WList_name', 'Operation', 'Well_1', 'Well_2', 'Well_3', 'Well_4', 'Well_5', 'Well_6', 'Well_7', 'Well_8', 'Well_9', 'Well_10', 'Well_11', 'Well_12']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WLIST', columns=key_col)
data = data.fillna('').copy()

for i in range(data.shape[0]):
    d = data[i:i+1]
    d.name = 'WLIST'
    write(df=d, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Well list keyword, WLIST, has written to schedule file.')

#### COMPLUMP ################################################################################
excelColumns = 'E:L'
data = pd.read_excel(schedule_input, sheet_name='COMPLUMP', skiprows=3,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'I', 'J', 'K-upper', 'K-lower', 'Number']l
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='COMPLUMP', columns=key_col)

int_args = {'I':'int32', 'J':'int32', 'K-upper':'int32', 'K-lower':'int32', 'Number':'int32'}
write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True, intArgs=int_args)
print('Lumps connections keyword, COMPLUMP, has written to schedule file.')

#### Drilling schedule ####################################################################
if ffORph1 == 'ff':
    excelColumns = 'E:R'
    startrow = 3
elif ffORph1 == 'ph1':
    excelColumns = 'AD:AQ'
    startrow = 4

ru = {'Base':'RU1', 'Optimistic':'RU2', 'Pessimistic':'RU3'}

data = pd.read_excel(schedule_input, sheet_name='wellOpening', skiprows=startrow, usecols=excelColumns).dropna(axis=0, how='all')
data['Cum_RampUp'] = data[ru[ramp_up]].cumsum()

data = startup_date(data, phase1start, predStart, phase2start)

ds = {'Base':'Days1', 'Optimistic':'Days2', 'Pessimistic':'Days3'}
date = []
for i in data.index:
    date1 = dt.timedelta(days=data.loc[i,ds[schedule]]) + data.loc[i, 'ref_date']
    date2 = dt.timedelta(days=data.loc[i,'Cum_RampUp']) + phase1start
    if data.Well_name.loc[i].startswith('*'):
        date.append(date1)
    else:
        date.append(max(date1, date2))
    
data['date'] = date

data = data[['date', 'Well_name', 'Status', 'I', 'J', 'K', '#1stComp', '#lastComp']].copy()
data.set_index('date', inplace=True)
data.sort_index(inplace=True)
data.name = 'WELOPEN'

# Exporting the actuall schedule for the specific realization tobe used as reference date.
drilling_schedule = data[['Well_name']].copy().reset_index()
startDates = pd.DataFrame(data={'date':[phase1start, phase2start], 'Well_name':['phase1start', 'phase2start']})
drilling_schedule = pd.concat([drilling_schedule, startDates], ignore_index=False, sort=False).set_index('date')
drilling_schedule.to_csv('drilling_schedule.csv')

int_args = int_args = {'#1stComp':'int32', '#lastComp':'int32'}
write(df=data, draft=draft_file, master_schedule=master_schedule, intArgs=int_args)
print('Drilling shedule has written to schedule file')

#### Group tree ################################################################################
excelColumns = 'D:G'
data = pd.read_excel(schedule_input, sheet_name='GRUPTREE', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Child', 'Parent']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='GRUPTREE', columns=key_col)

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Group tree has written to schedule file.')

#### WTEST ################################################################################
excelColumns = 'E:K'
data = pd.read_excel(schedule_input, sheet_name='WTEST', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'Interval', 'Reason', '#time', 'Startup']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WTEST', columns=key_col)

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Periodic testing, WTEST, has written to schedule file.')

#### WVFPEXP ################################################################################
excelColumns = 'E:J'
data = pd.read_excel(schedule_input, sheet_name='WVFPEXP', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'Imp/Exp', 'closeForVFP?', 'CTRLchange?']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WVFPEXP', columns=key_col)

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('WVFPEXP, has written to schedule file.')

#### WECON ################################################################################
excelColumns = 'E:U'
data = pd.read_excel(schedule_input, sheet_name='WECON', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'minOrate', 'minGrate', 'maxWCT', 'maxGOR', 'maxWGR', 'WOprocedure', 'flag', 'open_well', 'minEco', '2maxWCT', 'WOaction', 'maxGLR', 'minLrate', 'maxT']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WECON', columns=key_col)

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Economic limit data, WECON, has written to schedule file.')

#### WEFAC ################################################################################
excelColumns = 'E:I'
data = pd.read_excel(schedule_input, sheet_name='WEFAC', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'efficiency', 'network']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WEFAC', columns=key_col)

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Well efficiency factors, WEFAC, has written to schedule file.')

#### COMPORD & NUPCOL & GUIDERAT ##################################################################
excelColumns = 'E:H'
compord = pd.read_excel(schedule_input, sheet_name='COMPORD&NUPCOL&GUIDERAT', skiprows=2, usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'L:N'
nupcol = pd.read_excel(schedule_input, sheet_name='COMPORD&NUPCOL&GUIDERAT', skiprows=3, usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'R:AD'
guideRate = pd.read_excel(schedule_input, sheet_name='COMPORD&NUPCOL&GUIDERAT', skiprows=4, usecols=excelColumns).dropna(axis=0, how='all')

key_col_compord = ['date', 'Well_name', 'Method']
compord = prepare_data(df=compord, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='COMPORD', columns=key_col_compord)
key_col_nupcol = ['date', '#iteration']
nupcol = prepare_data(df=nupcol, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='NUPCOL', columns=key_col_nupcol)
key_col_guideRate = ['date', 'minTimeInt', 'phase', 'A', 'B', 'C', 'D', 'E', 'F', 'increase?', 'damp', 'freeGas']
guideRate = prepare_data(df=guideRate, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='GUIDERAT', columns=key_col_guideRate)

write(df=compord, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Defines the ordering of well connections, COMPORD, has written to schedule file.')
int_args = {'#iteration':'int32'}
write(df=nupcol, draft=draft_file, master_schedule=master_schedule, last_slash=False, intArgs=int_args)
print('Number of iterations to update well targets, NUPCOL, has written to schedule file.')
write(df=guideRate, draft=draft_file, master_schedule=master_schedule, last_slash=False)
print('GUIDERAT keyword has written to schedule file.')

#### Production network ##################################################################
excelColumns = 'E:J'
branch = pd.read_excel(schedule_input, sheet_name='Network', skiprows=2,  usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'O:U'
node = pd.read_excel(schedule_input, sheet_name='Network', skiprows=3,  usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'Z:AF'
netbal = pd.read_excel(schedule_input, sheet_name='Network', skiprows=4,  usecols=excelColumns).dropna(axis=0, how='all')

key_col_branch = ['date', 'Downtree', 'Uptree', '#VFP', 'ALQ']
branch = prepare_data(df=branch, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='BRANPROP', columns=key_col_branch)
key_col_node = ['date', 'Node_name', 'Pr', 'autoChock?', 'addGasLift?', 'Group_name']
node = prepare_data(df=node, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='NODEPROP', columns=key_col_node)
key_col_netbal = ['date', 'Interval', 'TolerancePr', 'maxIter', 'ToleranceT', 'maxIterTHP']
netbal = prepare_data(df=netbal, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='NETBALAN', columns=key_col_netbal)

int_args = {'#VFP':'int32'}
write(df=branch, draft=draft_file, master_schedule=master_schedule, last_slash=True, intArgs=int_args)
write(df=node, draft=draft_file, master_schedule=master_schedule, last_slash=True)
int_args = {'maxIter':'int32', 'maxIterTHP':'int32'}
write(df=netbal, draft=draft_file, master_schedule=master_schedule, last_slash=False, intArgs=int_args)
print('Production network keywords, BRANPROP NODEPROP NETBALAN, has written to schedule file.')

#### Group_control ################################################################################
if ffORph1 == 'ff':
    sheetname = 'Group_Ctrl_ff'
elif ffORph1 == 'ph1':
    sheetname = 'Group_Ctrl_ph1'
    
excelColumns = 'E:U'
production = pd.read_excel(schedule_input, sheet_name=sheetname, skiprows=2,  usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'AB:AJ'
lift = pd.read_excel(schedule_input, sheet_name=sheetname, skiprows=3,  usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'AO:BB'
injection = pd.read_excel(schedule_input, sheet_name=sheetname, skiprows=4,  usecols=excelColumns).dropna(axis=0, how='all')

key_col_production = ['date', 'Group', 'Ctrl', 'Orate', 'Wrate', 'Grate', 'Lrate', 'exceedRate', 'higherLevel', 'grupGR', 'DguideRate', 'exceedWrate', 'exceedGrate', 'exceedLrate', 'ResVolRate', 'ResVolFrac']
production = prepare_data(df=production, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='GCONPROD', columns=key_col_production)
key_col_injection = ['date', 'Group', 'phase', 'mode', 'Srate', 'totalRate', 'reinjection', 'voidage', 'higherLevel', 'grupGR', 'guideRate', 'reinjFrac', 'voidFrac']
injection = prepare_data(df=injection, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='GCONINJE', columns=key_col_injection)

liftMax = {'Base':'maxLiftGas1', 'Optimistic':'maxLiftGas2', 'Pessimistic':'maxLiftGas3'}
liftTotal = {'Base':'totalGas1', 'Optimistic':'totalGas2', 'Pessimistic':'totalGas3'}
key_col_lift = ['date', 'Group', liftMax[gconprod], liftTotal[gconprod]]
lift = prepare_data(df=lift, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='GLIFTOPT', columns=key_col_lift)

write(df=injection, draft=draft_file, master_schedule=master_schedule, last_slash=True)
write(df=lift, draft=draft_file, master_schedule=master_schedule, last_slash=True)
write(df=production, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Group control keywords, GCONPROD GCONINJE GLIFTOPT, has written to schedule file.')

#### Include_files ###########################################################################
#if ffORph1 == 'ff':
#    excelColumns = 'E:K'
excelColumns = 'E:K'

#elif ffORph1 == 'ph1':
#    excelColumns = 'O:T'
#else:
#    print('Please select either "ff" or "ph1".')


header = pd.read_excel(schedule_input, sheet_name='Include_files', skiprows=1, nrows=0, usecols=excelColumns)#, keep_default_na=False)
header = [str(h) for h in header]
data = pd.read_excel(schedule_input, sheet_name='Include_files', skiprows=2, names=header, usecols=excelColumns).dropna(axis=0, how='all')

if ffORph1 == 'ph1':
    data = data.loc[data['Ref']!='ph2start']

data = startup_date(data, phase1start, predStart, phase2start) 

data['date'] = pd.to_timedelta(data.Days, unit='d') + data['ref_date']

data['content'] = "'" + data['Path'] + data[gconprod] + "'" 

data = data[['date', 'content']]
data.set_index('date', inplace=True)
data.sort_index(inplace=True)
data.name = 'INCLUDE'

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=False)
print('Include-files has written to schedule file.')

#### Gas lift ################################################################################
excelColumns = 'E:M'
wlift = pd.read_excel(schedule_input, sheet_name='Gaslift', skiprows=2,  usecols=excelColumns).dropna(axis=0, how='all')
excelColumns = 'R:W'
lift = pd.read_excel(schedule_input, sheet_name='Gaslift', skiprows=3,  usecols=excelColumns).dropna(axis=0, how='all')

key_col_wlift = ['date', 'Well_name', 'Lift_gas_opt?', 'maxRate', 'preferentialFac', 'minRate', 'GrateFac', 'additional?']
wlift = prepare_data(df=wlift, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WLIFTOPT', columns=key_col_wlift)
key_col_lift = ['date', 'Size', 'minOrate', 'max_Interval', 'optNUPCOL?']
lift = prepare_data(df=lift, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='LIFTOPT', columns=key_col_lift)

write(df=lift, draft=draft_file, master_schedule=master_schedule, last_slash=False)
write(df=wlift, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Gas lift keywords, WLIFTOPT & LIFTOPT, has written to schedule file.')

#### WCONPROD ################################################################################
excelColumns = 'E:R'
data = pd.read_excel(schedule_input, sheet_name='WCONPROD', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'Status', 'Ctrl', 'Orate', 'Wrate', 'Grate', 'Lrate', 'RFV', 'FBHP', 'WHP', 'VFP', 'Glift']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WCONPROD', columns=key_col)

int_args = {'VFP':'int32'}
write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True, intArgs=int_args)
print('Producers control keyword, WCONPROD, has written to schedule file.')

#### Injection control ################################################################################
excelColumns = 'E:Q'
injection = pd.read_excel(schedule_input, sheet_name='WCONINJE', skiprows=3,  usecols=excelColumns).dropna(axis=0, how='all')

SRate = {'Base':'SRate1', 'Optimistic':'SRate2', 'Pessimistic':'SRate3'}
key_col_injection = ['date', 'Well_name', 'Type', 'Status', 'Ctrl', SRate[injectionRate], 'Rrate', 'BHP', 'THP', 'VFP']
injection = prepare_data(df=injection, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WCONINJE', columns=key_col_injection, well_start=drilling_schedule.reset_index())

write(df=injection, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Injection control keyword, WCONINJE, has written to schedule file.')

#### WGRUPCON ################################################################################
excelColumns = 'E:K'
data = pd.read_excel(schedule_input, sheet_name='WGRUPCON', skiprows=1,  usecols=excelColumns).dropna(axis=0, how='all')

key_col = ['date', 'Well_name', 'grupCtrl?', 'guideRate', 'phase', 'scalling']
data = prepare_data(df=data, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WGRUPCON', columns=key_col)

write(df=data, draft=draft_file, master_schedule=master_schedule, last_slash=True)
print('Well guide rates for group control keyword, WGRUPCON, has written to schedule file.')

#### PI ################################################################################
# excelColumns = 'E:H'
# welpi = pd.read_excel(schedule_input, sheet_name='PI', skiprows=2,  usecols=excelColumns).dropna(axis=0, how='all')
# excelColumns = 'L:T'
# wpimult = pd.read_excel(schedule_input, sheet_name='PI', skiprows=3,  usecols=excelColumns).dropna(axis=0, how='all')

# key_col_welpi = ['date', 'Well_name', 'Value']
# welpi = prepare_data(df=welpi, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WELPI', columns=key_col_welpi)
# key_col_wpimult = ['date', 'Well_name', 'Multiplier', 'I', 'J', 'K', '#1stCompl', '#lastCompl']
# wpimult = prepare_data(df=wpimult, phase1start=phase1start, predStart=predStart, phase2start=phase2start, data_name='WPIMULT', columns=key_col_wpimult)

# write(df=wpimult, draft=draft_file, master_schedule=master_schedule, last_slash=True)
# write(df=welpi, draft=draft_file, master_schedule=master_schedule, last_slash=True)
# print('Well productivity/injectivity index values, WELPI, has written to schedule file.')

### upTime ################################################################################
columnsName  = 'E:J'
if ffORph1 == 'ff':
    excelColumns = 'E:J'
elif ffORph1 == 'ph1':
    excelColumns = 'O:T'
else:
    print('Please select either "ff" or "ph1".')

gefac_header = pd.read_excel(schedule_input, sheet_name = 'upTime', skiprows=1, nrows=0 , usecols=columnsName).columns.values
gefac_header = [str(header) for header in gefac_header]

upTime = pd.read_excel(schedule_input, sheet_name = 'upTime', skiprows=1, names=gefac_header, usecols=excelColumns).dropna(how='all')

upTime = startup_date(upTime,  phase1start, predStart, phase2start)
upTime['date'] = pd.to_timedelta(upTime.Days, unit='d') + upTime['ref_date']
upTime.set_index('date', inplace=True)
upTime = upTime[['Group', gefac]]

upTime.sort_index(inplace=True)
upTime.name = 'GEFAC'
upTime['The_rest'] = '1*'

write(df=upTime, draft=draft_file, master_schedule=master_schedule)
print('GEFAC has written to schedule file')

### #######################################################################################
shutil.copyfile(master_schedule, '%s_orig' %master_schedule)   #copy the original file to _orig for comparison
shutil.copyfile(draft_file, master_schedule)            #copy the tmp_file with the new values to the input file name.

if os.path.isfile(draft_file):
    os.remove(draft_file)  

# Everything worked OK. Give ERT the message
with open(target_fn, 'w') as f:
    f.write('OK')
