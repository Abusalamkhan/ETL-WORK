#importing libraries

import pandas as pd
import pandasql as ps
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets
import warnings
warnings.filterwarnings("ignore")

#creating connections

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)

clients = pygsheets.authorize(service_file='client_secret.json')


#sheet keys

car_status_report='1CfqvArNmTofvNOFAhQ965B7cMA7lX40e3RZZKY6IUjI'
commitment_mapping_key='14zYk5KJ5J8uOxTlHvFF5uf-h_dRUa98x7FOl0AdbvIk'
aatmanirbhar3='1cC9dKh4tvSHQuL5ncSJgxD79IyeYlVYwSx-mU1Hz3fI'
fleet_driver='1qceRS8LU17n5YWvgewcpJZif02KxV8Mtvs6anomHD80'
allotment_status_report='1cpR6AVVpk9TF4_I38IFYPPOqk-_bSROHgVVYdaXLXOI'
car_master='1_r5OMN1P8Tof5IRaE5jYd-jHaP8j15avQVMycDJdhec'
uber_ws='C:\\Users\\sagar\\Dropbox\\DM Dashboard\\Master View.xlsx' 
dps_ws='C:\\Users\\sagar\\Dropbox\\DM Dashboard\\Driver Performance Sheet.xlsx'
car_servicing_schedule_calling_servicing_tab='11WVBiisNIF8Xb7sx7GZEok7mG4Lz3uo96-e00xVtW0M'
commitment_mapping_3='19bL6499hwfIWBi3igrkSGmkJpsPGczhxt7ykIYOpjU0'

###########################################################################################################################

#car_status_report

sheet= client.open_by_key(car_status_report)
ws= sheet.worksheet('Cars')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
dfs=dfm.loc[dfm['Type'].isin(['Revenue Share'])]
reject_df=dfs.loc[dfs['Current DM'] != 'Pune']
car_status_df=reject_df.iloc[:,[1,0,4]]
car_status_df.rename({'Current DM':'Team Name'},axis=1,inplace=True)
car_status_df

#commitment mapping Dashboard

sheet= client.open_by_key(commitment_mapping_key)
ws= sheet.worksheet('Dashboard Pasting')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
commitment_mapping_dashboard_df=dfm.iloc[:,[1,3]]
commitment_mapping_dashboard_df.rename({'Car No':'car_number','SDM Name':'Fleet_Lead'},axis=1,inplace=True)
commitment_mapping_dashboard_df

#aatmanirbhar3

sheet= client.open_by_key(aatmanirbhar3)
ws= sheet.worksheet('Fleet_Mapping')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
aatmanirbhar_3_df=dfm.iloc[:,[0,1]]
aatmanirbhar_3_df.rename({'License plate':'car_number'},axis=1,inplace=True)
aatmanirbhar_3_df

#fleet_driver

sheet= client.open_by_key(fleet_driver)
ws= sheet.worksheet('Fleet_driver')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
dfs=dfm.loc[dfm['city_id'].isin(['1'])]
fleet_driver_df=dfs.iloc[:,[2,3,5]]
fleet_driver_df.rename({'employee_id':'ETM','name':'Pilot Name','mobile':'Mobile Number'},axis=1,inplace=True)
fleet_driver_df

#allotment_status_report

sheet= client.open_by_key(allotment_status_report)
ws= sheet.worksheet('Allotment Status Report')
data = ws.get_all_values()
headers = data.pop(0)
df = pd.DataFrame(data,columns=headers)
allt_df=df.iloc[:,[2,17]]
allt_df['ID']=allt_df['ID'].str.upper()
allot_df=allt_df[allt_df.ID.str.startswith('ETM')]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
allot_df['Timestamp']=pd.to_datetime(allot_df['Timestamp'])
df_thisweek = allot_df[(allot_df['Timestamp'] >= pd.to_datetime(previous_monday)) & (allot_df['Timestamp'] <= pd.to_datetime(today))]
df_1_week=df_thisweek.groupby(['ID'],as_index=True)['Timestamp'].min()
allotment_s_r=pd.DataFrame(df_1_week).reset_index()
allotment_s_r.rename({'ID':'ETM','Timestamp':'Rating'},axis=1,inplace=True)
allotment_s_r['Rating']='New'
allotment_s_r

#commitment_mapping_calling_last call

sheet= client.open_by_key(commitment_mapping_key)
ws= sheet.worksheet('Calling')
data = ws.get_all_values()
headers = data.pop(0)
commitment_mapping_calling_tab = pd.DataFrame(data,columns=headers)
commitment_mapping=commitment_mapping_calling_tab.loc[:,['Timestamp','ETM ID']]
commitment_mapping_calling_tab_last_call=commitment_mapping.groupby(['ETM ID'],as_index=True)['Timestamp'].max()
commitment_mapping_calling_tab_last_call_df=pd.DataFrame(commitment_mapping_calling_tab_last_call).reset_index()
commitment_mapping_calling_tab_last_call_df.rename({'ETM ID':'ETM','Timestamp':'Last Call'},axis=1,inplace=True)
commitment_mapping_calling_tab_last_call_df

#commitment_mapping_todays_call

sheet= client.open_by_key(commitment_mapping_key)
ws= sheet.worksheet('Calling')
data = ws.get_all_values()
headers = data.pop(0)
commitment_mapping_calling_tab = pd.DataFrame(data,columns=headers)
commitment_mapping=commitment_mapping_calling_tab.iloc[:,[0,3,9]]
today = datetime.date.today()
commitment_mapping['Timestamp']=pd.to_datetime(commitment_mapping['Timestamp'], errors='coerce',format='%d/%m/%Y %H:%M:%S')
commitment_mapping['Timestamp'] = commitment_mapping['Timestamp'].dt.strftime('%Y-%m-%d')
df_thisweek = commitment_mapping[(commitment_mapping['Timestamp'] == today.strftime('%Y-%m-%d'))]
commitment_mapping_calling_tab_today_call=df_thisweek.loc[:,['ETM ID','Driver Status?']]
commitment_mapping_calling_tab_today_call.rename({'ETM ID':'ETM','Driver Status?':'Todays Call'},axis=1,inplace=True)
commitment_mapping_calling_tab_today_call

#commitment_mapping_calling_yesterday_call

sheet= client.open_by_key(commitment_mapping_key)
ws= sheet.worksheet('Calling')
data = ws.get_all_values()
headers = data.pop(0)
commitment_mapping_calling_tab = pd.DataFrame(data,columns=headers)
commitment_mapping=commitment_mapping_calling_tab.iloc[:,[0,3,9]]
today = datetime.date.today()
yesterday = today- timedelta(days=1)
commitment_mapping['Timestamp']=pd.to_datetime(commitment_mapping['Timestamp'], errors='coerce',format='%d/%m/%Y %H:%M:%S')
commitment_mapping['Timestamp'] = commitment_mapping['Timestamp'].dt.strftime('%Y-%m-%d')
df_thisweek = commitment_mapping[(commitment_mapping['Timestamp'] == yesterday.strftime('%Y-%m-%d'))]
commitment_mapping_calling_tab_yesterday_call=df_thisweek.loc[:,['ETM ID','Driver Status?']]
commitment_mapping_calling_tab_yesterday_call.rename({'ETM ID':'ETM','Driver Status?':'Yesterday Call'},axis=1,inplace=True)
commitment_mapping_calling_tab_yesterday_call

#uber_df_ND_column

uber_df_nd= pd.read_excel(uber_ws,sheet_name=-1)#uber sheet
uber_df_nd_df=uber_df_nd.iloc[:,[0,3,4,5,6,7,8,9]]
today = datetime.date.today()
today_timestmp_column = today.strftime("%Y-%m-%d %H:%M:%S")
yesterday = today- timedelta(days=1)
yesterday_timestmp_column=yesterday.strftime("%Y-%m-%d %H:%M:%S")
monday = today - datetime.timedelta(days=today.weekday())
monday_timestmp_column=monday.strftime("%Y-%m-%d %H:%M:%S")
if today==monday:
    uber_column_nd=uber_df_nd_df.iloc[:,[0]]
    uber_column_nd['ND']='0'
    uber_column_nd.rename({'Car No':'car_number'},axis=1,inplace=True)
    uber_column_nd
else:
    column_range=pd.date_range(monday_timestmp_column,yesterday_timestmp_column)
    uber_column=pd.concat([uber_df_nd_df['Car No'],uber_df_nd[column_range.to_list()]],axis=1)
    df__ = []
    for i in uber_column.index:        
        df__.append(list(uber_column[column_range.to_list()].values[i]).count('ND'))
    uber_column['ND'] = df__
    uber_column_nd=uber_column.loc[:,['Car No','ND']]
    uber_column_nd.rename({'Car No':'car_number'},axis=1,inplace=True)
    uber_column_nd

#uber_df_today_yesterday

uber_df = pd.read_excel(uber_ws,sheet_name=-1)#uber sheet
today = datetime.date.today()
today_timestmp_column = today.strftime("%Y-%m-%d %H:%M:%S")
yesterday = today- timedelta(days=1)
yesterday_timestmp_column=yesterday.strftime("%Y-%m-%d %H:%M:%S")
uber_dfs=uber_df.loc[:,['Car No','Last Week Trips',pd.to_datetime(today_timestmp_column),pd.to_datetime(yesterday_timestmp_column),'Total Trips','Balance','Hours Online','Net Fare']]
uber_dfs.rename({'Car No':'car_number',pd.to_datetime(today_timestmp_column):'Todays Trips',pd.to_datetime(yesterday_timestmp_column):'Yesterdays Trips','Balance':'Balanced Trips','Hours Online':'Online Hours','Net Fare':'Revenue Total'},axis=1,inplace=True)
uber_dfs

#car_servicing_schedule_calling_servicing_tab

sheet= client.open_by_key(car_servicing_schedule_calling_servicing_tab)
ws= sheet.worksheet('Calling Servicing Pending')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
servicing_df=dfm.iloc[:,[1,0]]
servicing_df.rename({'Car Number':'car_number','':'Servicing'},axis=1,inplace=True)
servicing_df

#car_master

sheet= client.open_by_key(car_master)
ws= sheet.worksheet('Driver Hisaab Final')
data = ws.get_all_values()
headers = data.pop(0)
car_master_final= pd.DataFrame(data,columns=headers)
car_master_final_df=car_master_final.iloc[:,[0,1]]
car_master_final_df.rename({'Driver ETM':'ETM'},axis=1,inplace=True)
car_master_final_df

#dps_dead_km_yesterday

dps_dfs = pd.read_excel(dps_ws,sheet_name=0)#dps sheet
today = datetime.date.today()
yesterday = today- timedelta(days=1)
yesterday_timestmp_column=yesterday.strftime("%d-%m-%Y")
dps_dfs['Date']=pd.to_datetime(dps_dfs['Date'])
df_thisweek = dps_dfs[(dps_dfs['Date'] == yesterday_timestmp_column)]
dead_km_yesterday=df_thisweek.loc[:,['Car no','Dead KMs']]
dead_km_yesterday.rename({'Car no':'car_number','Dead KMs':'Dead KM yesterday'},axis=1,inplace=True)
dead_km_yesterday

#dps_dead_km_this_week

dps_dfs = pd.read_excel(dps_ws,sheet_name=0)#dps sheet
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
dps_dfs['Date']=pd.to_datetime(dps_dfs['Date'])
df_this_week = dps_dfs[(dps_dfs['Date'] >= pd.to_datetime(previous_monday)) & (dps_dfs['Date'] < pd.to_datetime(today))]
dps_dead_km_this_week=df_this_week.iloc[:,[1,8]]
dps_dead_km_this_week['Dead KMs']=pd.to_numeric(dps_dead_km_this_week['Dead KMs'], errors = 'coerce')
dfs=dps_dead_km_this_week.groupby(['Car no'],as_index=True)['Dead KMs'].sum()
dkmtw=pd.DataFrame(dfs).reset_index()
dkmtw.rename({'Car no':'car_number','Dead KMs':'Dead KM'},axis=1,inplace=True)
dkmtw

#dps_dead_km_previous_monday_to_previous_sunday

dps_dfs = pd.read_excel(dps_ws,sheet_name=0)#dps sheet
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=7)
previous_sunday_week= previous_monday - datetime.timedelta(days=1)
dps_dfs['Date']=pd.to_datetime(dps_dfs['Date'])
df_this_week = dps_dfs[(dps_dfs['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_dfs['Date'] <= pd.to_datetime(previous_sunday_week))]
dps_dead_km_this_week=df_this_week.iloc[:,[1,8]]
dps_dead_km_this_week['Dead KMs']=pd.to_numeric(dps_dead_km_this_week['Dead KMs'], errors = 'coerce')
dfs=dps_dead_km_this_week.groupby(['Car no'],as_index=True)['Dead KMs'].sum()
Last_Week_Dead_KM=pd.DataFrame(dfs).reset_index()
Last_Week_Dead_KM.rename({'Car no':'car_number','Dead KMs':'Last Week Dead KM'},axis=1,inplace=True)
Last_Week_Dead_KM

###########################################################################################################################

#mapping car status and commitment dashboard pasting

cs_cmdf=car_status_df.merge(commitment_mapping_dashboard_df, on='car_number',how='left')
cs_cmdf_df=cs_cmdf.iloc[:,[3,0,1,2]]
cs_cmdf_df

#mapping aatmanirbhar 3 fleet mapping with previous merge

cs_cmdf_atm3=cs_cmdf_df.merge(aatmanirbhar_3_df, on='car_number',how='left')
cs_cmdf_atm3_df=cs_cmdf_atm3.iloc[:,[0,1,2,4,3]]
cs_cmdf_atm3_df

#mapping fleet_driver fleet_driver tab with previous merge

cs_cmdf_atm3_flt_df=cs_cmdf_atm3_df.merge(fleet_driver_df,on='ETM',how='left')
cs_cmdf_atm3_flt_df

#mapping allotment status report tab with previous merge

cs_cmdf_atm3_flt_allt_df=cs_cmdf_atm3_flt_df.merge(allotment_s_r,on='ETM',how='left')
cs_cmdf_atm3_flt_allt_df

#mapping commitment_mapping_calling_tab_last_call_df tab with previous merge

cs_cmdf_atm3_flt_allt_cmlc_df=cs_cmdf_atm3_flt_allt_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
cs_cmdf_atm3_flt_allt_cmlc_df

#mapping commitment_mapping_calling_tab_today_call_df tab with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_df=cs_cmdf_atm3_flt_allt_cmlc_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_df

#mapping commitment_mapping_calling_tab_yesterday_call_df tab with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_df

#mapping uber not driven tab with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_df.merge(uber_column_nd,on='car_number',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_df

#mapping uber dataframe tab with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_df.merge(uber_dfs,on='car_number',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_df

#mapping servicing tab with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_df.merge(servicing_df,on='car_number',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_df

#mapping car_master_final_df with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_df.merge(car_master_final_df,on='ETM',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_df

#mapping dead_km_yesterday with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_df.merge(dead_km_yesterday,on='car_number',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_df

#mapping dkmtw with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_dkmtw_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_df.merge(dkmtw,on='car_number',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_dkmtw_df

#mapping Last_Week_Dead_KM with previous merge

cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_dkmtw_lwdk_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_dkmtw_df.merge(Last_Week_Dead_KM,on='car_number',how='left')
cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_dkmtw_lwdk_df

#master_df

master_df=cs_cmdf_atm3_flt_allt_cmlc_cmtc_cmyc_u_nd_uber_servicing_final_dky_dkmtw_lwdk_df.fillna('')
master_df

#matching ratings column

master_df['Last Week Trips']=master_df['Last Week Trips'].astype(str)
aa= []
for i in master_df.index:
    if master_df['Rating'].values[i]=='New':
        aa.append('New')
    elif master_df['Last Week Trips'].values[i] == '0.0':
        aa.append('ND')
    elif master_df['Last Week Trips'].values[i] < '20.0':
        aa.append('Piker')
    elif master_df['Last Week Trips'].values[i] < '28.0':
        aa.append('Laggard')
    elif master_df['Last Week Trips'].values[i] < '50.0':
        aa.append('Mediocre')
    elif master_df['Last Week Trips'].values[i] >= '50.0':
        aa.append('Performer')

master_df['Rating'] =aa
master_df

#pushing to commitment mapping 3.0 sheets

sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear()
ws.set_dataframe(master_df,(1,1))