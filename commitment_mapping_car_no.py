#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets
import warnings
warnings.filterwarnings("ignore")

#creating connections

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('car-master-sheet.json', scope) 
client= gspread.authorize(credentials)

clients = pygsheets.authorize(service_file='car-master-sheet.json')


#sheet keys

car_status_report='1CfqvArNmTofvNOFAhQ965B7cMA7lX40e3RZZKY6IUjI'
fleet_driver='1qceRS8LU17n5YWvgewcpJZif02KxV8Mtvs6anomHD80'
car_master='1_r5OMN1P8Tof5IRaE5jYd-jHaP8j15avQVMycDJdhec'
# uber_ws='C:\\Users\\sagar\\Dropbox\\Dropbox\\DM Dashboard\\Master View.xlsx' 
# dps_ws='C:\\Users\\sagar\\Dropbox\\Dropbox\\DM Dashboard\\Driver Performance Sheet.xlsx'
car_servicing_schedule_calling_servicing_tab='11WVBiisNIF8Xb7sx7GZEok7mG4Lz3uo96-e00xVtW0M'
commitment_mapping_3='1sy3Gxrnh8bX6ibpGR6X8rba8H2IfeSNdBGT7CIFi3SY'
wtd_sheet='1E9U_nG61vCYxbc3yQJHEKpIPfYuQxolllthf8weEckU'

#5 sheets keys

terrific='1f-DJ5O3zKKkAtXfXQghWzbsoXyy-ipdJ5q7Yma4tF9M'
roaring='1MiZZY9MPEhhg-B9LyfmThQGV3Dy3XnKpb5NqgaYFwIQ'
silent='1ZwXyZPyt7qhjTO5VkfwoGTJ48fmg_38MTdy4EHdVhwA'
deep='1TPRKPsQfy4qY19byAuYi0Ie3Xld7ddrrBS8xqhggtcM'
black='1KD0ABDWp3YvqIkmoQAZKRS-7-S8i-HtlO4KKpBMB1tU'
car_no_master_list='11D8_6u4ywy3yNYyrnMonyti6eflqUtpMfsxgLMBXEzk'

##################################################sheets extraction#######################################################

#car_status_report
sheet= client.open_by_key(car_status_report)
ws= sheet.worksheet('Cars')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
dfs=dfm.loc[dfm['Type'].isin(['Revenue Share'])]
dfs['Current DM']=dfs['Current DM'].str.title()
reject_df=dfs.loc[dfs['Current DM'] != 'Pune']

sheet= client.open_by_key(car_no_master_list)
ws= sheet.worksheet('DM')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
mumbai_60_40=dfm[dfm['Cities']=='Mumbai 60:40']

all_team=reject_df[reject_df['Current DM'].isin(mumbai_60_40['DM NAME'])]
all_team
car_status_df=all_team.iloc[:,[1,0,4]]
car_status_df['ETM']=car_status_df['ETM'].str.strip()
car_status_df.rename({'Current DM':'Team Name','Car Number':'car_number'},axis=1,inplace=True)
car_status_df

#fleet_column_df
fleet_column_df=all_team.iloc[:,[0,5]]
fleet_column_df.rename({'Car Number':'car_number','Fleet ID':'Fleet'},axis=1,inplace=True)
fleet_column_df.fillna('',inplace=True)
fleet_column_df

#fleet_lead_col_df

fleet_lead_col_df=all_team.iloc[:,[0,6]]
fleet_lead_col_df.rename({'Car Number':'car_number','Fleet Lead':'Fleet_Lead'},axis=1,inplace=True)
fleet_lead_col_df.fillna('',inplace=True)
fleet_lead_col_df

#uber_df_ND_column

uber_df= pd.read_excel(uber_ws,sheet_name=-1)#uber sheet

uber_df_nd_df=uber_df.iloc[:,[0,3,4,5,6,7,8,9]]
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
    uber_column=pd.concat([uber_df['Car No'],uber_df[column_range.to_list()]],axis=1)
    df__ = []
    for i in uber_column.index:        
        df__.append(list(uber_column[column_range.to_list()].values[i]).count('ND'))
    uber_column['ND'] = df__
    uber_column_nd=uber_column.loc[:,['Car No','ND']]
    uber_column_nd.rename({'Car No':'car_number'},axis=1,inplace=True)
    uber_column_nd
    
#uber_df_today_yesterday

today = datetime.date.today()
monday = today - datetime.timedelta(days=today.weekday())
today_timestmp_column = today.strftime("%Y-%m-%d %H:%M:%S")
ttc=today_timestmp_column+'.1'
yesterday = today- timedelta(days=1)
yesterday_timestmp_column=yesterday.strftime("%Y-%m-%d %H:%M:%S")
ytc=yesterday_timestmp_column+'.1'
if today==monday:
    uber_df[ytc]='0'
    uber_dfs=uber_df.loc[:,['Car No','Last Week Trips',(ytc),'Total Trips','Balance','Hours Online','Net Fare']]
    uber_dfs.rename({'Car No':'car_number',(ytc):'Yesterdays Trips','Balance':'Balanced Trips','Hours Online':'Online Hours','Net Fare':'Revenue Total'},axis=1,inplace=True)
    uber_dfs
else:
    uber_dfs=uber_df.loc[:,['Car No','Last Week Trips',(ytc),'Total Trips','Balance','Hours Online','Net Fare']]
    uber_dfs.rename({'Car No':'car_number',(ytc):'Yesterdays Trips','Balance':'Balanced Trips','Hours Online':'Online Hours','Net Fare':'Revenue Total'},axis=1,inplace=True)
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

#dps_dead_km_yesterday

dps_dfs = pd.read_excel(dps_ws,sheet_name=0)#dps sheet

today = datetime.date.today()
today_timestmp_column=today.strftime("%d-%m-%Y")
yesterday = today- timedelta(days=1)
yesterday_timestmp_column=yesterday.strftime("%Y-%m-%d")
dps_dfs['Date']=pd.to_datetime(dps_dfs['Date'])
df_thisweek = dps_dfs[(dps_dfs['Date'] == yesterday_timestmp_column)]
dead_km_yesterday=df_thisweek.loc[:,['Car no','Dead KMs']]
dead_km_yesterday.rename({'Car no':'car_number','Dead KMs':'Dead KM yesterday'},axis=1,inplace=True)
dead_km_yesterday

#dps_dead_km_this_week

previous_monday = today - datetime.timedelta(days=today.weekday())
dps_dfs['Date']=pd.to_datetime(dps_dfs['Date'])
df_this_week = dps_dfs[(dps_dfs['Date'] >= pd.to_datetime(previous_monday)) & (dps_dfs['Date'] < pd.to_datetime(today))]
dps_dead_km_this_week=df_this_week.iloc[:,[1,8]]
dps_dead_km_this_week['Dead KMs']=pd.to_numeric(dps_dead_km_this_week['Dead KMs'], errors = 'coerce')
dfs=dps_dead_km_this_week.groupby(['Car no'],as_index=True)['Dead KMs'].sum()
dkmtw=pd.DataFrame(dfs).reset_index()
dkmtw.rename({'Car no':'car_number','Dead KMs':'This week Dead KM'},axis=1,inplace=True)
dkmtw

#dps_dead_km_previous_monday_to_previous_sunday

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

#uber_df_accecptance_cancelation

# uber_df= pd.read_excel(uber_ws,sheet_name=-1)
uber_df_a_c_df=uber_df.loc[:,['Car No','Acceptance','Cancellation']]
uber_df_a_c_df.rename({'Car No':'car_number'},axis=1,inplace=True)
uber_df_a_c_df.fillna('',inplace=True)
uber_df_a_c_df

############################################################Car status pushing############################################################

#car_status_df 2 columns

car_status_df_2_column=car_status_df.iloc[:,[0,1]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='B',end='C')
ws.set_dataframe(car_status_df_2_column,(1,2))

#car_status_df 1 columns

car_status_df_1_column=car_status_df.iloc[:,[2]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='E',end='E')
ws.set_dataframe(car_status_df_1_column,(1,5))

# # ################################################# mapping car  status on the basis of car number and pushing ##################################################################

# fleet_lead_col_df

cs_cmdf=car_status_df.merge(fleet_lead_col_df, on='car_number',how='left')
cs_cmdf_df=cs_cmdf.iloc[:,[3]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='A',end='A')
ws.set_dataframe(cs_cmdf_df,(1,1))

# fleet_column_df

cs_atm3=car_status_df.merge(fleet_column_df, on='car_number',how='left')
cs_atm3_df=cs_atm3.iloc[:,[3]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='D',end='D')
ws.set_dataframe(cs_atm3_df,(1,4))

# #uber nd column

cs_uber_nd=car_status_df.merge(uber_column_nd, on='car_number',how='left')
cs_uber_nd_df=cs_uber_nd.iloc[:,[3]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='M',end='M')
ws.set_dataframe(cs_uber_nd_df,(1,13))

#uber_dfs

cs_uber_dfs=car_status_df.merge(uber_dfs, on='car_number',how='left')
cs_uber_dfs_df=cs_uber_dfs.iloc[:,[3,4,5,6,7,8]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='N',end='S')
ws.set_dataframe(cs_uber_dfs_df,(1,14))

#car_servicing_schedule_calling_servicing

cs_servicing=car_status_df.merge(servicing_df, on='car_number',how='left')
cs_servicing_d=cs_servicing.iloc[:,[3]]
cs_servicing_df=cs_servicing_d.fillna('')
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='U',end='U')
ws.set_dataframe(cs_servicing_df,(1,21))

#dead km yesterday

dead_km_yesterday_df=car_status_df.merge(dead_km_yesterday, on='car_number',how='left')
cs_dky_df=dead_km_yesterday_df.iloc[:,[3]]
cs_dky=cs_dky_df.fillna('')
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='W',end='W')
ws.set_dataframe(cs_dky,(1,23))

#dkmtw

dkmtw_df=car_status_df.merge(dkmtw, on='car_number',how='left')
cs_dkmtw=dkmtw_df.iloc[:,[3]]
cs_dkmtw_df=cs_dkmtw.fillna('')
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='X',end='X')
ws.set_dataframe(cs_dkmtw_df,(1,24))

#Last_Week_Dead_KM

lwdk_df=car_status_df.merge(Last_Week_Dead_KM, on='car_number',how='left')
lwdk_dfs=lwdk_df.iloc[:,[3]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='Y',end='Y')
ws.set_dataframe(lwdk_dfs,(1,25))

#uber_df_accecptance_cancelation

uber_df_acceptnce_cancellation=car_status_df.merge(uber_df_a_c_df, on='car_number',how='left')
uber_df_acceptnce_cancellation_df=uber_df_acceptnce_cancellation.iloc[:,[3,4]]
uber_df_acceptnce_cancellation_dfs=uber_df_acceptnce_cancellation_df.fillna('')
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
ws.clear(start='AA',end='AB')
ws.set_dataframe(uber_df_acceptnce_cancellation_dfs,(1,27))

# # #######################################################pushing uber to uber all sheets

#uber

uber_dfs_tab=uber_df.iloc[:,[0,1,2,39,3,4,5,6,7,8,9,45,17,18,19,20,21,22,23,44,24,25,26,27,28,29,30]]
uber_dfs_tab.columns=uber_dfs_tab.columns.astype(str)
uber_dfs_tab
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(uber_dfs_tab,(1,1))

#terrific
uber_df_terrific= uber_dfs_tab[uber_dfs_tab["DM"].isin(["Terrific Tigers"])]
uber_df_terrific.columns=uber_df_terrific.columns.astype(str)
uber_df_terrific
sheet= clients.open_by_key(terrific)
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(uber_df_terrific,(1,1))

#roaring

uber_df_roaring= uber_dfs_tab[uber_dfs_tab["DM"].isin(["Roaring Lions"])]
uber_df_roaring.columns=uber_df_roaring.columns.astype(str)
uber_df_roaring
sheet= clients.open_by_key(roaring)
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(uber_df_roaring,(1,1))

# #silent

uber_df_silent= uber_dfs_tab[uber_dfs_tab["DM"].isin(["Silent Killers"])]
uber_df_silent.columns=uber_df_silent.columns.astype(str)
uber_df_silent
sheet= clients.open_by_key(silent)
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(uber_df_silent,(1,1))

# #deep

uber_df_deep= uber_dfs_tab[uber_dfs_tab["DM"].isin(["Deep Hunters"])]
uber_df_deep.columns=uber_df_deep.columns.astype(str)
uber_df_deep
sheet= clients.open_by_key(deep)
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(uber_df_deep,(1,1))

# #black

uber_df_black= uber_dfs_tab[uber_dfs_tab["DM"].isin(["Black Panthers"])]
uber_df_black.columns=uber_df_black.columns.astype(str)
uber_df_black
sheet= clients.open_by_key(black)
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(uber_df_black,(1,1))


# ######################################################################################dps to alls sheets

#dps

today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= today - datetime.timedelta(days=14)
dps_dfs['Date']=pd.to_datetime(dps_dfs['Date'])
df_this_week = dps_dfs[(dps_dfs['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_dfs['Date'] <= pd.to_datetime(today))]
sheet= clients.open_by_key(commitment_mapping_3)#master
ws= sheet.worksheet_by_title('DPS')
ws.clear()
ws.set_dataframe(df_this_week,(1,1))

#terrific

dps_df_terrific= dps_dfs[dps_dfs["DM name"].isin(["Terrific Tigers"])]
dps_df_terrific['Date']=pd.to_datetime(dps_df_terrific['Date'])
df_this_week_terrefic = dps_df_terrific[(dps_df_terrific['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_df_terrific['Date'] <= pd.to_datetime(today))]
df_this_week_terrefic
sheet= clients.open_by_key(terrific)
ws= sheet.worksheet_by_title('DPS')
ws.clear()
ws.set_dataframe(df_this_week_terrefic,(1,1))

# #roaring

dps_df_roaring= dps_dfs[dps_dfs["DM name"].isin(["Roaring Lions"])]
dps_df_roaring['Date']=pd.to_datetime(dps_df_roaring['Date'])
df_this_week_roaring = dps_df_roaring[(dps_df_roaring['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_df_roaring['Date'] <= pd.to_datetime(today))]
df_this_week_roaring
sheet= clients.open_by_key(roaring)
ws= sheet.worksheet_by_title('DPS')
ws.clear()
ws.set_dataframe(df_this_week_roaring,(1,1))

#silent 

dps_df_silent= dps_dfs[dps_dfs["DM name"].isin(["Silent Killers"])]
dps_df_silent['Date']=pd.to_datetime(dps_df_silent['Date'])
df_this_week_silent = dps_df_silent[(dps_df_silent['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_df_silent['Date'] <= pd.to_datetime(today))]
df_this_week_silent
sheet= clients.open_by_key(silent)
ws= sheet.worksheet_by_title('DPS')
ws.clear()
ws.set_dataframe(df_this_week_silent,(1,1))

#deep

dps_df_deep= dps_dfs[dps_dfs["DM name"].isin(["Deep Hunters"])]
dps_df_deep['Date']=pd.to_datetime(dps_df_deep['Date'])
df_this_week_deep = dps_df_deep[(dps_df_deep['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_df_deep['Date'] <= pd.to_datetime(today))]
df_this_week_deep
sheet= clients.open_by_key(deep)
ws= sheet.worksheet_by_title('DPS')
ws.clear()
ws.set_dataframe(df_this_week_deep,(1,1))

# #black

dps_df_black= dps_dfs[dps_dfs["DM name"].isin(["Black Panthers"])]
dps_df_black['Date']=pd.to_datetime(dps_df_black['Date'])
df_this_week_black = dps_df_black[(dps_df_black['Date'] >= pd.to_datetime(previous_monday_week)) & (dps_df_black['Date'] <= pd.to_datetime(today))]
df_this_week_black
sheet= clients.open_by_key(black )
ws= sheet.worksheet_by_title('DPS')
ws.clear()
ws.set_dataframe(df_this_week_black,(1,1))

# ########################################################################uber_dps_dead km to dead km all sheets

#uber_deadkm

uber_df.columns=uber_df.columns.astype(str)
uber_dps_df=uber_df.iloc[:,[0,1,2,50,31,32,33,34,35,36,37]]
sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Dead KM')
ws.clear()
ws.set_dataframe(uber_dps_df,(1,1))

# #terrific 

uber_dfs_dead= uber_dps_df[uber_dps_df["DM"].isin(["Terrific Tigers"])]
sheet= clients.open_by_key(terrific)
ws= sheet.worksheet_by_title('Dead KM')
ws.clear()
ws.set_dataframe(uber_dfs_dead,(1,1))

# #roaring

uber_dfs_dead= uber_dps_df[uber_dps_df["DM"].isin(["Roaring Lions"])]
sheet= clients.open_by_key(roaring)
ws= sheet.worksheet_by_title('Dead KM')
ws.clear()
ws.set_dataframe(uber_dfs_dead,(1,1))

# #silent

uber_dfs_dead= uber_dps_df[uber_dps_df["DM"].isin(["Silent Killers"])]
sheet= clients.open_by_key(silent)
ws= sheet.worksheet_by_title('Dead KM')
ws.clear()
ws.set_dataframe(uber_dfs_dead,(1,1))

# #deep 

uber_dfs_dead= uber_dps_df[uber_dps_df["DM"].isin(["Deep Hunters"])]
sheet= clients.open_by_key(deep)
ws= sheet.worksheet_by_title('Dead KM')
ws.clear()
ws.set_dataframe(uber_dfs_dead,(1,1))

# #black

uber_dfs_dead= uber_dps_df[uber_dps_df["DM"].isin(["Black Panthers"])]
sheet= clients.open_by_key(black)
ws= sheet.worksheet_by_title('Dead KM')
ws.clear()
ws.set_dataframe(uber_dfs_dead,(1,1))

# ########################################################pushing to 5 sheets###########################################

#terrific

sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Master')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)

terific_dfs = dfm[dfm["Team Name"].isin(["Terrific Tigers"])]
terific_df=terific_dfs.iloc[:,0:19]
sheet= clients.open_by_key(terrific)
ws= sheet.worksheet_by_title('Terrific_Tigers')
ws.clear(start='A',end='S')
ws.set_dataframe(terific_df,(1,1))


terific_dfs = dfm[dfm["Team Name"].isin(["Terrific Tigers"])]
terific_df_1=terific_dfs.iloc[:,20:28]
sheet= clients.open_by_key(terrific)
ws= sheet.worksheet_by_title('Terrific_Tigers')
ws.clear(start='U',end='AB')
ws.set_dataframe(terific_df_1,(1,21))

#roaring

Roaring_dfs= dfm[dfm["Team Name"].isin(["Roaring Lions"])]
Roaring_df=Roaring_dfs.iloc[:,0:19]
sheet= clients.open_by_key(roaring)
ws= sheet.worksheet_by_title('Roaring_Lions')
ws.clear(start='A',end='S')
ws.set_dataframe(Roaring_df,(1,1))

Roaring_dfs= dfm[dfm["Team Name"].isin(["Roaring Lions"])]
Roaring_df_1=Roaring_dfs.iloc[:,20:28]
sheet= clients.open_by_key(roaring)
ws= sheet.worksheet_by_title('Roaring_Lions')
ws.clear(start='U',end='AB')
ws.set_dataframe(Roaring_df_1,(1,21))

#silent

Silent_dfs= dfm[dfm["Team Name"].isin(["Silent Killers"])]
Silent_df=Silent_dfs.iloc[:,0:19]
sheet= clients.open_by_key(silent)
ws= sheet.worksheet_by_title('Silent_Killers')
ws.clear(start='A',end='S')
ws.set_dataframe(Silent_df,(1,1))

Silent_dfs= dfm[dfm["Team Name"].isin(["Silent Killers"])]
Silent_df_1=Silent_dfs.iloc[:,20:28]
sheet= clients.open_by_key(silent)
ws= sheet.worksheet_by_title('Silent_Killers')
ws.clear(start='U',end='AB')
ws.set_dataframe(Silent_df_1,(1,21))

# deep

Deep_Hunters_dfs= dfm[dfm["Team Name"].isin(["Deep Hunters"])]
Deep_Hunters_df=Deep_Hunters_dfs.iloc[:,0:19]
sheet= clients.open_by_key(deep)
ws= sheet.worksheet_by_title('Deep_Hunters')
ws.clear(start='A',end='S')
ws.set_dataframe(Deep_Hunters_df,(1,1))

Deep_Hunters_df= dfm[dfm["Team Name"].isin(["Deep Hunters"])]
Deep_Hunters_df_1=Deep_Hunters_df.iloc[:,20:28]
sheet= clients.open_by_key(deep)
ws= sheet.worksheet_by_title('Deep_Hunters')
ws.clear(start='U',end='AB')
ws.set_dataframe(Deep_Hunters_df_1,(1,21))

# black

Black_Panthers_dfs= dfm[dfm["Team Name"].isin(["Black Panthers"])]
Black_Panthers_df=Black_Panthers_dfs.iloc[:,0:19]
sheet= clients.open_by_key(black)
ws= sheet.worksheet_by_title('Black_Panthers')
ws.clear(start='A',end='S')
ws.set_dataframe(Black_Panthers_df,(1,1))

Black_Panthers_df= dfm[dfm["Team Name"].isin(["Black Panthers"])]
Black_Panthers_df_1=Black_Panthers_df.iloc[:,20:28]
sheet= clients.open_by_key(black)
ws= sheet.worksheet_by_title('Black_Panthers')
ws.clear(start='U',end='AB')
ws.set_dataframe(Black_Panthers_df_1,(1,21))

#wtd trips script

wtd_df=uber_df[['Total Trips','Car No']]
sheet= clients.open_by_key(wtd_sheet)
ws= sheet.worksheet_by_title('Trips_Data')
ws.set_dataframe(wtd_df,(1,1))
print("wtd trips data tab updated succesfully")

print("Car mapping car no updated succesfully")