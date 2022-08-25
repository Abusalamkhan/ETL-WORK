import os,sys;sys.path.append(os.path.dirname(os.path.realpath('mail')))
from mail.send_mail import sendMail
import pygsheets #Importing python in google sheets
import pandas as pd #importing pandas
from pandas import DataFrame
import datetime as dt
from datetime import datetime, timedelta,date
import numpy as np
import datetime

import warnings
warnings.filterwarnings('ignore')

try:
# gc = pygsheets.authorize(service_file='car-master-sheet.json')

    gc = pygsheets.authorize(service_file='/home/karan/config/car-master-sheet.json')

    today = date.today() + timedelta(days = 1)

    allotment_wkbk = gc.open("Allotment Status Report - Pune")


    allotment_history_sheet = allotment_wkbk.worksheet_by_title("New Allotment History")

    allotment_history_data = pd.DataFrame(allotment_history_sheet.get_all_records())

    allotment_history_data = allotment_history_data[['Car + Count','Car Number','Allotment Date','ETM','Return Date']]

    allotment_history_data['Return Date'] = allotment_history_data['Return Date'].replace(r'^\s*$', today, regex=True)

    allotment_history_data = allotment_history_data.replace(r'^\s*$', np.nan, regex=True)

    allotment_history_data = allotment_history_data[allotment_history_data['ETM'].str.contains('ET',na=False)]

    allotment_history_data['Allotment Date'] = pd.to_datetime(allotment_history_data['Allotment Date'], dayfirst = True)

    allotment_history_data['Return Date'] = pd.to_datetime(allotment_history_data['Return Date'], dayfirst = True)
    allotment_history_data

    allotment_history_data['Allotment Date Dummy'] = allotment_history_data['Allotment Date'].dt.date

    allotment_history_data['Return Date Dummy'] = allotment_history_data['Return Date'].dt.date

    allotment_history_data = allotment_history_data.loc[~(allotment_history_data['Allotment Date Dummy'] == allotment_history_data['Return Date Dummy'])]

    adjustment_given_allotment_history = allotment_history_data[['Car Number','Return Date Dummy','ETM']]

    adjustment_taken_allotment_history = allotment_history_data[['Car + Count','ETM','Allotment Date Dummy','Allotment Date']]

    audit_data = allotment_wkbk.worksheet_by_title("Audit")

    allotment_audit_data = pd.DataFrame(audit_data.get_all_records())

    allotment_audit_data = allotment_audit_data[['Car + Count','Car Number','CNG - How many bars','Fuel indicator Petrol','Adjustment Amount','Timestamp']]

    allotment_audit_data = allotment_audit_data.replace(r'^\s*$', np.nan, regex=True)

    allotment_audit_data = allotment_audit_data[allotment_audit_data['Fuel indicator Petrol'].notna()]

    allotment_audit_data['Timestamp'] = pd.to_datetime(allotment_audit_data['Timestamp'], dayfirst = True)

    allotment_audit_data['start_date'] = pd.Timestamp('08-05-2022')

    allotment_audit_data = allotment_audit_data.loc[(allotment_audit_data['Timestamp'] > allotment_audit_data['start_date'])]

    allotment_audit_data = allotment_audit_data.loc[~(allotment_audit_data['Adjustment Amount'] == 0)]

    allotment_audit_data['Return Date Dummy'] = allotment_audit_data['Timestamp'].dt.date

    allotment_audit_data.drop(['start_date'],axis = 1, inplace = True)
    allotment_audit_data

    adjustment_given = pd.merge(allotment_audit_data,adjustment_given_allotment_history,on=['Return Date Dummy','Car Number'],how='inner')

    adjustment_given.rename(columns={"Return Date Dummy":"Bill Date"},inplace = True)

    adjustment_given.drop(['Car + Count'],axis = 1, inplace = True)

    adjustment_given['start_date'] = pd.Timestamp('08-05-2022')

    adjustment_given = adjustment_given.loc[(adjustment_given['Timestamp'] > adjustment_given['start_date'])]

    adjustment_given.drop(['start_date'],axis = 1, inplace = True)
    adjustment_given

    allotment_audit_data.drop(['Return Date Dummy','Timestamp'],axis = 1, inplace = True)

    adjustment_taken = pd.merge(allotment_audit_data,adjustment_taken_allotment_history,on='Car + Count',how='inner')
    adjustment_taken['Adjustment Amount New'] = adjustment_taken['Adjustment Amount'] * -1

    adjustment_taken.drop(['Car + Count','Adjustment Amount'],axis = 1, inplace = True)

    adjustment_taken.rename(columns={"Allotment Date Dummy":"Bill Date","Allotment Date":"Timestamp","Adjustment Amount New":"Adjustment Amount"},inplace = True)




    print(adjustment_given.head())
    print(adjustment_taken.head())


    adjustment_final = pd.concat([adjustment_taken, adjustment_given], ignore_index=True)


    adjustment_final['Name of DM'] = 'Allotment'
    adjustment_final['Pilot Name'] = ''
    adjustment_final['Adjustment Reason'] = 'Excess Petrol/CNG' 
    adjustment_final['Adjustment Comment'] = "CNG:" + adjustment_final['CNG - How many bars'].astype(str) + " / Petrol: " + adjustment_final['Fuel indicator Petrol'].astype(str)
    adjustment_final.drop(['CNG - How many bars','Fuel indicator Petrol'],axis = 1, inplace = True)

    adjustment_final.sort_values(by=['Timestamp'], ascending=True, inplace=True)

    adjustment_final

    adjustment_final['Car Number']=adjustment_final['Car Number'].str.replace(' ','')

    car_no_master_list = gc.open("Car No Master list")
    car_no_master_list_history_tab =car_no_master_list.worksheet_by_title("History")
    car_no_master_list_history_tab_data = pd.DataFrame(car_no_master_list_history_tab.get_all_records())
    car_no_master_list_history_tab_data['Car No']=car_no_master_list_history_tab_data['Car No'].str.replace(' ','')
    car_no_master_list_history_tab_data['Date']=pd.to_datetime(car_no_master_list_history_tab_data['Date'], errors='coerce',format='%d-%b-%Y')
    car_no_master_list_history_tab_data['End']=pd.to_datetime(car_no_master_list_history_tab_data['End'], errors='coerce',format='%d/%m/%Y')
    car_no_master_list_history_tab_data.rename({'Car No':'Car Number'},axis=1,inplace=True)
    car_no_master_dm_name=adjustment_final.merge(car_no_master_list_history_tab_data, on='Car Number',how='left')
    car_no_master_dm_name_df = car_no_master_dm_name[(car_no_master_dm_name['Date'] <= car_no_master_dm_name['Timestamp']) & (car_no_master_dm_name['Timestamp'] <= car_no_master_dm_name['End'])]
    car_no_master_dm_name_df=car_no_master_dm_name_df[['Car Number','DM','ETM','Bill Date','Timestamp','Adjustment Amount','Name of DM','Pilot Name','Adjustment Reason','Adjustment Comment']]
    car_no_master_dm_name_df


    adjustment_gs = gc.open("Pune Adjustment Sheet")

    adjustment_sheet = adjustment_gs.worksheet_by_title("New Allotment Fuel")

    adjustment_sheet.set_dataframe(car_no_master_dm_name_df, 'A1')

except Exception:
    sendMail([
    'abusalameverestfleet@gmail.com',
    'avinash.everestfleet@gmail.com',
    'rutvijaevrestfleet@gmail.com',
    'sameer.everestfleet@gmail.com',
    'Karan.saraogi@everestfleet.com',
    'vijugode@gmail.com'
    ])