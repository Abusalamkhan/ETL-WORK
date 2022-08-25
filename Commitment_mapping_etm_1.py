#importing libraries

import pandas as pd
import numpy as np
import pandasql as ps
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets
import warnings
warnings.filterwarnings("ignore")
from twilio.rest import Client as c


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
commitment_mapping_3='1sy3Gxrnh8bX6ibpGR6X8rba8H2IfeSNdBGT7CIFi3SY'

#5 sheet keys

terrific='1f-DJ5O3zKKkAtXfXQghWzbsoXyy-ipdJ5q7Yma4tF9M'
roaring='1MiZZY9MPEhhg-B9LyfmThQGV3Dy3XnKpb5NqgaYFwIQ'
silent='1ZwXyZPyt7qhjTO5VkfwoGTJ48fmg_38MTdy4EHdVhwA'
deep='1TPRKPsQfy4qY19byAuYi0Ie3Xld7ddrrBS8xqhggtcM'
black='1KD0ABDWp3YvqIkmoQAZKRS-7-S8i-HtlO4KKpBMB1tU'

#########################################################################sheets
try:
    
    
    #fleet_driver
    sheet= client.open_by_key(fleet_driver)
    ws= sheet.worksheet('Fleet_driver')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    dfs=dfm.loc[dfm['city_id'].isin(['1'])]
    fleet_driver_dfs=dfs.iloc[:,[2,3,5,6]]
    fleet_driver_df=fleet_driver_dfs.loc[fleet_driver_dfs['employee_id'] != '']
    fleet_driver_df['employee_id']=fleet_driver_df['employee_id'].str.strip()
    fleet_driver_df.rename({'employee_id':'ETM','name':'Pilot Name','mobile':'Mobile Number'},axis=1,inplace=True)
    fleet_driver_df

    #allotment_status_report

    sheet= client.open_by_key(allotment_status_report)
    ws= sheet.worksheet('Allotment Status Report')
    data = ws.get_all_values()
    headers = data.pop(0)
    df = pd.DataFrame(data,columns=headers)
    allt_df=df.loc[:,['Timestamp','ID']]
    allt_df['ID']=allt_df['ID'].str.upper()
    allot_df=allt_df[allt_df.ID.str.startswith('ETM')]
    today = datetime.date.today()
    previous_monday = today - datetime.timedelta(days=today.weekday())
    previous_monday_week= previous_monday - datetime.timedelta(days=7)
    allot_df['Timestamp']=pd.to_datetime(allot_df['Timestamp'], errors='coerce',format='%d/%m/%Y %H:%M:%S')
    df_thisweek = allot_df[(allot_df['Timestamp'] >= pd.to_datetime(previous_monday_week)) & (allot_df['Timestamp'] <= pd.to_datetime(today))]
    df_1_week=df_thisweek.groupby(['ID'],as_index=True)['Timestamp'].min()
    allotment_s_r=pd.DataFrame(df_1_week).reset_index()
    allotment_s_r.rename({'ID':'ETM','Timestamp':'Rating'},axis=1,inplace=True)
    allotment_s_r['Rating']='New'
    allotment_s_r

    #commitment_mapping_calling_last call

    sheet= client.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet('Calling Data')
    data = ws.get_all_values()
    headers = data.pop(0)
    commitment_mapping_calling_tab = pd.DataFrame(data,columns=headers)
    commitment_mapping=commitment_mapping_calling_tab.loc[:,['Timestamp','ETM ID']]
    commitment_mapping_calling_tab_last_call=commitment_mapping.groupby(['ETM ID'],as_index=True)['Timestamp'].max()
    commitment_mapping_calling_tab_last_call_df=pd.DataFrame(commitment_mapping_calling_tab_last_call).reset_index()
    commitment_mapping_calling_tab_last_call_df.rename({'ETM ID':'ETM','Timestamp':'Last Call'},axis=1,inplace=True)
    commitment_mapping_calling_tab_last_call_df

    #commitment_mapping_todays_call

    sheet= client.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet('Calling Data')
    data = ws.get_all_values()
    headers = data.pop(0)
    commitment_mapping_calling_tab = pd.DataFrame(data,columns=headers)
    commitment_mapping=commitment_mapping_calling_tab.iloc[:,[0,3,7]]
    today = datetime.date.today()
    commitment_mapping['Timestamp']=pd.to_datetime(commitment_mapping['Timestamp'], errors='coerce',format='%d/%m/%Y %H:%M:%S')
    commitment_mapping['Timestamp'] = commitment_mapping['Timestamp'].dt.strftime('%Y-%m-%d')
    df_thisweek = commitment_mapping[(commitment_mapping['Timestamp'] == today.strftime('%Y-%m-%d'))]
    commitment_mapping_calling_tab_today_call=df_thisweek.loc[:,['ETM ID','Driver Status?']]
    commitment_mapping_calling_tab_today_call.rename({'ETM ID':'ETM','Driver Status?':'Todays Call'},axis=1,inplace=True)
    commitment_mapping_calling_tab_today_call

    #commitment_mapping_calling_yesterday_call

    sheet= client.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet('Calling Data')
    data = ws.get_all_values()
    headers = data.pop(0)
    commitment_mapping_calling_tab = pd.DataFrame(data,columns=headers)
    commitment_mapping=commitment_mapping_calling_tab.iloc[:,[0,3,7]]
    today = datetime.date.today()
    yesterday = today- timedelta(days=1)
    commitment_mapping['Timestamp']=pd.to_datetime(commitment_mapping['Timestamp'], errors='coerce',format='%d/%m/%Y %H:%M:%S')
    commitment_mapping['Timestamp'] = commitment_mapping['Timestamp'].dt.strftime('%Y-%m-%d')
    df_thisweek = commitment_mapping[(commitment_mapping['Timestamp'] == yesterday.strftime('%Y-%m-%d'))]
    commitment_mapping_calling_tab_yesterday_call=df_thisweek.loc[:,['ETM ID','Driver Status?']]
    commitment_mapping_calling_tab_yesterday_call.rename({'ETM ID':'ETM','Driver Status?':'Yesterday Call'},axis=1,inplace=True)
    commitment_mapping_calling_tab_yesterday_call

    #car_master

    sheet= client.open_by_key(car_master)
    ws= sheet.worksheet('Driver Hisaab Final')
    data = ws.get_all_values()
    headers = data.pop(0)
    car_master_final= pd.DataFrame(data,columns=headers)
    car_master_final_df=car_master_final.iloc[:,[0,1]]
    car_master_final_df.rename({'Driver ETM':'ETM'},axis=1,inplace=True)
    car_master_final_df

    # #allotment status report for allotment date

    sheet= client.open_by_key(allotment_status_report)
    ws= sheet.worksheet('Allotment Status Report')
    data = ws.get_all_values()
    headers = data.pop(0)
    df = pd.DataFrame(data,columns=headers)
    allt_df=df.loc[:,['Timestamp','Car Number','ID']]
    allt_df['ID']=allt_df['ID'].str.upper()
    allot_df=allt_df[allt_df.ID.str.startswith('ETM')]
    allot_max=allot_df.groupby(['Car Number','ID'],as_index=True)['Timestamp'].max()
    allot_max_dfm=pd.DataFrame(allot_max).reset_index()
    allot_max_dfm.rename({'ID':'ETM','Timestamp':'Allotment date'},axis=1,inplace=True)
    allot_max_df= allot_max_dfm[['Allotment date', 'ETM']].groupby(['ETM'])['Allotment date'].max()
    allot_max_df
    
###############################################################################pushing new etm mapping with car number

     #car_status_report

    sheet= client.open_by_key(car_status_report)
    ws= sheet.worksheet('Cars')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    dfs=dfm.loc[dfm['Type'].isin(['Revenue Share'])]
    dfs['Current DM']=dfs['Current DM'].str.title()
    reject_df=dfs.loc[dfs['Current DM'] != 'Pune']
    reject_df_1=reject_df.loc[reject_df['Current DM'] != 'Ev']
    car_status_df=reject_df_1.iloc[:,[1,0,4]]
    car_status_df['ETM']=car_status_df['ETM'].str.strip()
    car_status_df.rename({'Current DM':'Team Name','Car Number':'car_number'},axis=1,inplace=True)
    car_status_df

    #etm from master
    
    sheet= client.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet('Master')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    masterview_car_no=dfm.iloc[:,[2]]
    masterview_car_no
    
    etm_dfs=masterview_car_no.merge(car_status_df,on='car_number',how='left')
    new_etm=etm_dfs.iloc[:,[2]]  
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.clear(start='E',end='E')
    ws.set_dataframe(new_etm,(1,5))
       
    etm_dfs=masterview_car_no.merge(car_status_df,on='car_number',how='left')
    etm_terrific=etm_dfs[etm_dfs["Team Name"].isin(["Terrific Tigers"])]
    etm_terrific_df=etm_terrific.iloc[:,[2]]
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(etm_terrific_df,(1,5))

    etm_dfs=masterview_car_no.merge(car_status_df,on='car_number',how='left')
    etm_roaring=etm_dfs[etm_dfs["Team Name"].isin(["Roaring Lions"])]
    etm_roaring_df=etm_roaring.iloc[:,[2]]   
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(etm_roaring_df,(1,5))

    etm_dfs=masterview_car_no.merge(car_status_df,on='car_number',how='left')
    etm_silent=etm_dfs[etm_dfs["Team Name"].isin(["Silent Killers"])]
    etm_silent_df=etm_silent.iloc[:,[2]]   
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(etm_silent_df,(1,5))

    etm_dfs=masterview_car_no.merge(car_status_df,on='car_number',how='left')
    etm_deep=etm_dfs[etm_dfs["Team Name"].isin(["Deep Hunters"])]
    etm_deep_df=etm_deep.iloc[:,[2]] 
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(etm_deep_df,(1,5))

    etm_dfs=masterview_car_no.merge(car_status_df,on='car_number',how='left')
    etm_black=etm_dfs[etm_dfs["Team Name"].isin(["Black Panthers"])]
    etm_black_df=etm_black.iloc[:,[2]] 
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(etm_black_df,(1,5))          
        
               
    ###################################################################### master etm column
    
    #etm from master
    sheet= client.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet('Master')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    etm_df=dfm.iloc[:,[1,4]]
    etm_df['ETM']=etm_df['ETM'].str.strip()
    etm_df

    #last week trip from master
    sheet= client.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet('Master')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    last_week=dfm.iloc[:,[1,8,13]]
    last_week

#     ##################################################################### mapping on the basis of etm and 5 sheets

    #fleet_driver 

    cs_flt=etm_df.merge(fleet_driver_df,on='ETM',how='left')
    cs_flt_d=cs_flt.iloc[:,[2,3,4]]
    cs_flt_df=cs_flt_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_flt_df,(1,6))

    cs_flt=etm_df.merge(fleet_driver_df,on='ETM',how='left')
    cs_flt_terrefic=cs_flt[cs_flt["Team Name"].isin(["Terrific Tigers"])]
    cs_flt_d=cs_flt_terrefic.iloc[:,[2,3,4]]
    cs_flt_df_terrefic=cs_flt_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_flt_df_terrefic,(1,6))

    cs_flt=etm_df.merge(fleet_driver_df,on='ETM',how='left')
    cs_flt_roaring=cs_flt[cs_flt["Team Name"].isin(["Roaring Lions"])]
    cs_flt_d=cs_flt_roaring.iloc[:,[2,3,4]]
    cs_flt_df_roaring=cs_flt_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_flt_df_roaring,(1,6))

    cs_flt=etm_df.merge(fleet_driver_df,on='ETM',how='left')
    cs_flt_silent=cs_flt[cs_flt["Team Name"].isin(["Silent Killers"])]
    cs_flt_d=cs_flt_silent.iloc[:,[2,3,4]]
    cs_flt_df_silent=cs_flt_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_flt_df_silent,(1,6))

    cs_flt=etm_df.merge(fleet_driver_df,on='ETM',how='left')
    cs_flt_deep=cs_flt[cs_flt["Team Name"].isin(["Deep Hunters"])]
    cs_flt_d=cs_flt_deep.iloc[:,[2,3,4]]
    cs_flt_df_deep=cs_flt_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_flt_df_deep,(1,6)) 

    cs_flt=etm_df.merge(fleet_driver_df,on='ETM',how='left')
    cs_flt_black=cs_flt[cs_flt["Team Name"].isin(["Black Panthers"])]
    cs_flt_d=cs_flt_black.iloc[:,[2,3,4]]
    cs_flt_df_black=cs_flt_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_flt_df_black,(1,6))

    #rating

    cs_allot_rating=etm_df.merge(allotment_s_r,on='ETM',how='left')
    cs_allot_rating_d=cs_allot_rating.iloc[:,[2]]
    cs_allot_rating_df=cs_allot_rating_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_allot_rating_df,(1,9))

    cs_allot_rating=etm_df.merge(allotment_s_r,on='ETM',how='left')
    cs_allot_rating_terrefic=cs_allot_rating[cs_allot_rating["Team Name"].isin(["Terrific Tigers"])]
    cs_allot_rating_d=cs_allot_rating_terrefic.iloc[:,[2]]
    cs_allot_rating_df_terrific=cs_allot_rating_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_allot_rating_df_terrific,(1,9))

    cs_allot_rating=etm_df.merge(allotment_s_r,on='ETM',how='left')
    cs_allot_rating_terrefic=cs_allot_rating[cs_allot_rating["Team Name"].isin(["Roaring Lions"])]
    cs_allot_rating_d=cs_allot_rating_terrefic.iloc[:,[2]]
    cs_allot_rating_df_roaring=cs_allot_rating_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_allot_rating_df_roaring,(1,9))

    cs_allot_rating=etm_df.merge(allotment_s_r,on='ETM',how='left')
    cs_allot_rating_terrefic=cs_allot_rating[cs_allot_rating["Team Name"].isin(["Silent Killers"])]
    cs_allot_rating_d=cs_allot_rating_terrefic.iloc[:,[2]]
    cs_allot_rating_df_silent=cs_allot_rating_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_allot_rating_df_silent,(1,9))

    cs_allot_rating=etm_df.merge(allotment_s_r,on='ETM',how='left')
    cs_allot_rating_terrefic=cs_allot_rating[cs_allot_rating["Team Name"].isin(["Deep Hunters"])]
    cs_allot_rating_d=cs_allot_rating_terrefic.iloc[:,[2]]
    cs_allot_rating_df_deep=cs_allot_rating_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_allot_rating_df_deep,(1,9))

    cs_allot_rating=etm_df.merge(allotment_s_r,on='ETM',how='left')
    cs_allot_rating_terrefic=cs_allot_rating[cs_allot_rating["Team Name"].isin(["Black Panthers"])]
    cs_allot_rating_d=cs_allot_rating_terrefic.iloc[:,[2]]
    cs_allot_rating_df_black=cs_allot_rating_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_allot_rating_df_black,(1,9))

    #rating calculation

    last_week['Last Week Trips']=last_week['Last Week Trips'].replace(np.nan,0).replace('',0)
    last_week['Last Week Trips'] = last_week['Last Week Trips'].astype(float)
    aa= []
    for i in last_week.index:
        if last_week['Rating'].values[i]=='New':
            aa.append('New')
        elif last_week['Last Week Trips'].values[i] == 0:
            aa.append('ND')
        elif last_week['Last Week Trips'].values[i] < 30.0:
            aa.append('Piker')
        elif last_week['Last Week Trips'].values[i] < 55.0:
            aa.append('Laggard')
        elif last_week['Last Week Trips'].values[i] < 75.0:
            aa.append('Mediocre') 
        elif last_week['Last Week Trips'].values[i] < 95.0:
            aa.append('Performer')
        elif last_week['Last Week Trips'].values[i] >= 95.0:
            aa.append('out Performer')
        else:
            aa.append('')
    last_week['Rating']=aa[0:last_week.shape[0]]
    last_week_df=last_week.iloc[:,[1]]
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(last_week_df,(1,9))

    last_week_terrific=last_week[last_week["Team Name"].isin(["Terrific Tigers"])]
    last_week_df_terrefic=last_week_terrific.iloc[:,[1]]
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(last_week_df_terrefic,(1,9))

    last_week_roaring=last_week[last_week["Team Name"].isin(["Roaring Lions"])]
    last_week_df_roaring=last_week_roaring.iloc[:,[1]]
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(last_week_df_roaring,(1,9))

    last_week_silent=last_week[last_week["Team Name"].isin(["Silent Killers"])]
    last_week_df_silent=last_week_silent.iloc[:,[1]]
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(last_week_df_silent,(1,9))

    last_week_deep=last_week[last_week["Team Name"].isin(["Deep Hunters"])]
    last_week_df_deep=last_week_deep.iloc[:,[1]]
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(last_week_df_deep,(1,9))

    last_week_black=last_week[last_week["Team Name"].isin(["Black Panthers"])]
    last_week_df_black=last_week_black.iloc[:,[1]]
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(last_week_df_black,(1,9))

    #commitment_mapping_calling_last call

    cs_clc=etm_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
    cs_clc_d=cs_clc.iloc[:,[2]]
    cs_clc_df=cs_clc_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_clc_df,(1,10))

    cs_clc=etm_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
    cs_clc_terrefic=cs_clc[cs_clc["Team Name"].isin(["Terrific Tigers"])]
    cs_clc_d=cs_clc_terrefic.iloc[:,[2]]
    cs_clc_df_terrefic=cs_clc_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_clc_df_terrefic,(1,10))

    cs_clc=etm_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
    cs_clc_roaring=cs_clc[cs_clc["Team Name"].isin(["Roaring Lions"])]
    cs_clc_d=cs_clc_roaring.iloc[:,[2]]
    cs_clc_df_roaring=cs_clc_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_clc_df_roaring,(1,10))

    cs_clc=etm_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
    cs_clc_silent=cs_clc[cs_clc["Team Name"].isin(["Silent Killers"])]
    cs_clc_d=cs_clc_silent.iloc[:,[2]]
    cs_clc_df_silent=cs_clc_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_clc_df_silent,(1,10))

    cs_clc=etm_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
    cs_clc_deep=cs_clc[cs_clc["Team Name"].isin(["Deep Hunters"])]
    cs_clc_d=cs_clc_deep.iloc[:,[2]]
    cs_clc_df_deep=cs_clc_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_clc_df_deep,(1,10))

    cs_clc=etm_df.merge(commitment_mapping_calling_tab_last_call_df,on='ETM',how='left')
    cs_clc_black=cs_clc[cs_clc["Team Name"].isin(["Black Panthers"])]
    cs_clc_d=cs_clc_black.iloc[:,[2]]
    cs_clc_df_black=cs_clc_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_clc_df_black,(1,10))

    #commitment_mapping_calling_tab_today_call

    cs_ctc=etm_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
    cs_ctc_d=cs_ctc.iloc[:,[2]]
    cs_ctc_df=cs_ctc_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_ctc_df,(1,11))

    cs_ctc=etm_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
    cs_ctc_terrefic=cs_ctc[cs_ctc["Team Name"].isin(["Terrific Tigers"])]
    cs_ctc_d=cs_ctc_terrefic.iloc[:,[2]]
    cs_ctc_df_terrefic=cs_ctc_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_ctc_df_terrefic,(1,11))

    cs_ctc=etm_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
    cs_ctc_roaring=cs_ctc[cs_ctc["Team Name"].isin(["Roaring Lions"])]
    cs_ctc_d=cs_ctc_roaring.iloc[:,[2]]
    cs_ctc_df_roaring=cs_ctc_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_ctc_df_roaring,(1,11))

    cs_ctc=etm_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
    cs_ctc_silent=cs_ctc[cs_ctc["Team Name"].isin(["Silent Killers"])]
    cs_ctc_d=cs_ctc_silent.iloc[:,[2]]
    cs_ctc_df_silent=cs_ctc_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_ctc_df_silent,(1,11))

    cs_ctc=etm_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
    cs_ctc_deep=cs_ctc[cs_ctc["Team Name"].isin(["Deep Hunters"])]
    cs_ctc_d=cs_ctc_deep.iloc[:,[2]]
    cs_ctc_df_deep=cs_ctc_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_ctc_df_deep,(1,11))

    cs_ctc=etm_df.merge(commitment_mapping_calling_tab_today_call,on='ETM',how='left')
    cs_ctc_black=cs_ctc[cs_ctc["Team Name"].isin(["Black Panthers"])]
    cs_ctc_d=cs_ctc_black.iloc[:,[2]]
    cs_ctc_df_black=cs_ctc_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_ctc_df_black,(1,11))

    #commitment_mapping_calling_tab_yesterday_call

    cs_cyc=etm_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
    cs_cyc_d=cs_cyc.iloc[:,[2]]
    cs_cyc_df=cs_cyc_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_cyc_df,(1,12))

    cs_cyc=etm_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
    cs_cyc_dfs=cs_cyc[cs_cyc["Team Name"].isin(["Terrific Tigers"])]
    cs_cyc_d=cs_cyc_dfs.iloc[:,[2]]
    cs_cyc_df_teriffic=cs_cyc_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_cyc_df_teriffic,(1,12))

    cs_cyc=etm_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
    cs_cyc_dfs=cs_cyc[cs_cyc["Team Name"].isin(["Roaring Lions"])]
    cs_cyc_d=cs_cyc_dfs.iloc[:,[2]]
    cs_cyc_df_roaring=cs_cyc_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_cyc_df_roaring,(1,12))

    cs_cyc=etm_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
    cs_cyc_dfs=cs_cyc[cs_cyc["Team Name"].isin(["Silent Killers"])]
    cs_cyc_d=cs_cyc_dfs.iloc[:,[2]]
    cs_cyc_df_silent=cs_cyc_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_cyc_df_silent,(1,12))

    cs_cyc=etm_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
    cs_cyc_dfs=cs_cyc[cs_cyc["Team Name"].isin(["Deep Hunters"])]
    cs_cyc_d=cs_cyc_dfs.iloc[:,[2]]
    cs_cyc_df_deep=cs_cyc_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_cyc_df_deep,(1,12))

    cs_cyc=etm_df.merge(commitment_mapping_calling_tab_yesterday_call,on='ETM',how='left')
    cs_cyc_dfs=cs_cyc[cs_cyc["Team Name"].isin(["Black Panthers"])]
    cs_cyc_d=cs_cyc_dfs.iloc[:,[2]]
    cs_cyc_df_black=cs_cyc_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_cyc_df_black,(1,12))

    #car_master_final_df

    cs_final=etm_df.merge(car_master_final_df,on='ETM',how='left')
    cs_final_d=cs_final.iloc[:,[2]]
    cs_final_df=cs_final_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_final_df,(1,22))

    cs_final=etm_df.merge(car_master_final_df,on='ETM',how='left')
    cs_final_dfs=cs_final[cs_final["Team Name"].isin(["Terrific Tigers"])]
    cs_final_d=cs_final_dfs.iloc[:,[2]]
    cs_final_df_terrefic=cs_final_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_final_df_terrefic,(1,22))

    cs_final=etm_df.merge(car_master_final_df,on='ETM',how='left')
    cs_final_dfs=cs_final[cs_final["Team Name"].isin(["Roaring Lions"])]
    cs_final_d=cs_final_dfs.iloc[:,[2]]
    cs_final_df_roaring=cs_final_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_final_df_roaring,(1,22))

    cs_final=etm_df.merge(car_master_final_df,on='ETM',how='left')
    cs_final_dfs=cs_final[cs_final["Team Name"].isin(["Silent Killers"])]
    cs_final_d=cs_final_dfs.iloc[:,[2]]
    cs_final_df_silent=cs_final_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_final_df_silent,(1,22))

    cs_final=etm_df.merge(car_master_final_df,on='ETM',how='left')
    cs_final_dfs=cs_final[cs_final["Team Name"].isin(["Deep Hunters"])]
    cs_final_d=cs_final_dfs.iloc[:,[2]]
    cs_final_df_deep=cs_final_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_final_df_deep,(1,22))

    cs_final=etm_df.merge(car_master_final_df,on='ETM',how='left')
    cs_final_dfs=cs_final[cs_final["Team Name"].isin(["Black Panthers"])]
    cs_final_d=cs_final_dfs.iloc[:,[2]]
    cs_final_df_black=cs_final_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_final_df_black,(1,22))

    #allotment date

    cs_allot=etm_df.merge(allot_max_df,on='ETM',how='left')
    cs_allot_d=cs_allot.iloc[:,[2]]
    cs_allot_df=cs_allot_d.fillna('')
    sheet= clients.open_by_key(commitment_mapping_3)
    ws= sheet.worksheet_by_title('Master')
    ws.set_dataframe(cs_allot_df,(1,26))

    cs_allot=etm_df.merge(allot_max_df,on='ETM',how='left')
    cs_allots=cs_allot[cs_allot["Team Name"].isin(["Terrific Tigers"])]
    cs_allot_d=cs_allots.iloc[:,[2]]
    cs_allot_df_terrific=cs_allot_d.fillna('')
    sheet= clients.open_by_key(terrific)
    ws= sheet.worksheet_by_title('Terrific_Tigers')
    ws.set_dataframe(cs_allot_df_terrific,(1,26))

    cs_allot=etm_df.merge(allot_max_df,on='ETM',how='left')
    cs_allots=cs_allot[cs_allot["Team Name"].isin(["Roaring Lions"])]
    cs_allot_d=cs_allots.iloc[:,[2]]
    cs_allot_df_roaring=cs_allot_d.fillna('')
    sheet= clients.open_by_key(roaring)
    ws= sheet.worksheet_by_title('Roaring_Lions')
    ws.set_dataframe(cs_allot_df_roaring,(1,26))

    cs_allot=etm_df.merge(allot_max_df,on='ETM',how='left')
    cs_allots=cs_allot[cs_allot["Team Name"].isin(["Silent Killers"])]
    cs_allot_d=cs_allots.iloc[:,[2]]
    cs_allot_df_silent=cs_allot_d.fillna('')
    sheet= clients.open_by_key(silent)
    ws= sheet.worksheet_by_title('Silent_Killers')
    ws.set_dataframe(cs_allot_df_silent,(1,26))

    cs_allot=etm_df.merge(allot_max_df,on='ETM',how='left')
    cs_allots=cs_allot[cs_allot["Team Name"].isin(["Deep Hunters"])]
    cs_allot_d=cs_allots.iloc[:,[2]]
    cs_allot_df_deep=cs_allot_d.fillna('')
    sheet= clients.open_by_key(deep)
    ws= sheet.worksheet_by_title('Deep_Hunters')
    ws.set_dataframe(cs_allot_df_deep,(1,26))

    cs_allot=etm_df.merge(allot_max_df,on='ETM',how='left')
    cs_allots=cs_allot[cs_allot["Team Name"].isin(["Black Panthers"])]
    cs_allot_d=cs_allots.iloc[:,[2]]
    cs_allot_df_black=cs_allot_d.fillna('')
    sheet= clients.open_by_key(black)
    ws= sheet.worksheet_by_title('Black_Panthers')
    ws.set_dataframe(cs_allot_df_black,(1,26))

    print("Commitment mapping etm updated succesfully")

except Exception as e:
    number=['+91 81084 16708','+91 98200 66683']
    for to_number in number:
        account_sid="AC3459ee86068c97f9cd2de30ad98146e4"
        auth_token="208bac357247abfae8b25d89406dc608"
        client=c(account_sid,auth_token)
        from_number='+1 9705577507'
        error=str(e)
        error_message=error+" error in your commitment mapping etm code"        
        client.messages.create(
            body=error_message,
            from_=from_number,
            to=to_number)
        print(e,"error in your commitment mapping etm code")