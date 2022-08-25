import os, sys;sys.path.append(os.path.dirname(os.path.realpath('mail')))
from mail.send_mail import sendMail
import pygsheets  # Importing python in google sheets
import pandas as pd  # importing pandas
from pandas import Series, DataFrame
import datetime as dt
from datetime import datetime, timedelta
import numpy as np


# gc = pygsheets.authorize(service_file='/Users/karan_19981/Desktop/Work/Everest_Fleet/Database_Sheets/client_secret.json')

gc = pygsheets.authorize(service_file='/home/karan/config/car-master-sheet.json')

try:
    all_in_one_gs = gc.open("All  in One Form - Mumbai")
    all_in_one_responses_tab = all_in_one_gs.worksheet_by_title("Form Responses")
    all_in_one_responses = pd.DataFrame(all_in_one_responses_tab.get_all_records())


    car_recovery = all_in_one_responses.loc[(
        all_in_one_responses['Driver Left'] == 'Car Recovery')]


    car_recovery = car_recovery[['Timestamp', 'ETM',
                                'Team Name', 'Pilot Name', 'Car Number', 'Name of DM']]

    car_recovery = car_recovery.replace(r'^\s*$', np.nan, regex=True)

    car_recovery['Timestamp'] = pd.to_datetime(car_recovery['Timestamp'])


    car_recovery['start_date'] = pd.Timestamp('2022-07-06')
    car_recovery = car_recovery.loc[(
        car_recovery['Timestamp'] > car_recovery['start_date'])]


    car_recovery.drop(['start_date'], axis=1, inplace=True)

    print(car_recovery.head())


    car_recovery_gs = gc.open("Car Recovery Process")

    car_recovery_queries_tab = car_recovery_gs.worksheet_by_title(
        "Recovery Queries")

    car_recovery_queries = pd.DataFrame(car_recovery_queries_tab.get_all_records())


    car_recovery_queries = car_recovery_queries[[
        'Timestamp', 'Car Number', 'Biker alloted', 'Key', 'Time of Biker assign', 'Time of closing']]

    car_recovery_queries.rename(
        columns={"Car Number": "Car Number Checker"}, inplace=True)


    car_recovery_queries = car_recovery_queries.replace(
        r'^\s*$', np.nan, regex=True)

    car_recovery_queries['Timestamp'] = pd.to_datetime(
        car_recovery_queries['Timestamp'])


    car_recovery_penalty_query = pd.merge(
        car_recovery, car_recovery_queries, on='Timestamp', how='left')

    car_recovery_penalty_query = car_recovery_penalty_query[car_recovery_penalty_query['Car Number Checker'].notna(
    )]

    car_recovery_penalty_query.drop(['Car Number Checker'], axis=1, inplace=True)

    car_recovery_penalty = car_recovery_penalty_query[car_recovery_penalty_query['Biker alloted'].notna(
    )]

    car_recovery_penalty = car_recovery_penalty.loc[(car_recovery_penalty['Biker alloted'] != 'Driver') & (
        car_recovery_penalty['Biker alloted'] != 'Cancel')]

    car_recovery_penalty.drop(
        ['Key', 'Time of closing', 'Biker alloted'], axis=1, inplace=True)

    car_recovery_penalty['Time of Biker assign'] = pd.to_datetime(
        car_recovery_penalty['Time of Biker assign'])

    car_recovery_penalty.rename(
        columns={"Time of Biker assign": "Closure Time"}, inplace=True)

    car_recovery_penalty['Amount'] = 500


    print(car_recovery_penalty.tail(15))


    car_recovery_key_penalty = car_recovery_penalty_query[car_recovery_penalty_query['Key'].notna(
    )]

    car_recovery_key_penalty = car_recovery_key_penalty.loc[(
        car_recovery_key_penalty['Key'] == 'No')]

    car_recovery_key_penalty.drop(
        ['Biker alloted', 'Time of Biker assign', 'Key'], axis=1, inplace=True)

    car_recovery_key_penalty['Time of closing'] = pd.to_datetime(
        car_recovery_key_penalty['Time of closing'])

    car_recovery_key_penalty.rename(
        columns={"Time of closing": "Closure Time"}, inplace=True)

    car_recovery_key_penalty['Amount'] = 850


    key_lost_by = all_in_one_responses.loc[(
        all_in_one_responses['Key Lost By'] == 'Lost By Driver')]

    key_lost_by = key_lost_by[['Timestamp', 'ETM',
                            'Team Name', 'Pilot Name', 'Car Number', 'Name of DM']]

    key_lost_by['Amount'] = 850

    key_lost_by['Closure Time'] = key_lost_by['Timestamp']

    key_lost_by['Closure Time'] = pd.to_datetime(key_lost_by['Closure Time'])

    key_lost_by['Timestamp'] = pd.to_datetime(key_lost_by['Timestamp'])


    penalty_final = pd.concat(
        [car_recovery_key_penalty, car_recovery_penalty, key_lost_by], ignore_index=True)

    penalty_final.sort_values(by=['Closure Time'], ascending=True, inplace=True)

    print(penalty_final.tail())

    penalty_final['Remark'] = np.where((penalty_final['Amount'] == 500), "Car Recovery", "Key Lost")

    #########################################################################################################################

    #sheet keys
    penalty_sheet='1AiCX19-GU0KmA6x9h9X4s2YqCfhBtN4MH4oBljEWxnY'
    penalty_form_repair='1Txa0MVP7Kxxjhwjd5EwME3JfEPSi5q2klC7GuZw2hRc'
    fleet_driver='1qceRS8LU17n5YWvgewcpJZif02KxV8Mtvs6anomHD80'

    #penalty_form_repair
    penalty_form_repair_sheet = gc.open_by_key(penalty_form_repair)
    penalty_form_repair_penalty_amount_tab = penalty_form_repair_sheet.worksheet_by_title("Penalty_amount")
    penalty_form_repair_penalty_amount_data = pd.DataFrame(penalty_form_repair_penalty_amount_tab.get_all_records())

    #fleet_driver
    fleet_driver_sheet = gc.open_by_key(fleet_driver)
    fleet_driver_tab = fleet_driver_sheet.worksheet_by_title("Fleet_driver")
    fleet_driver_data = pd.DataFrame(fleet_driver_tab.get_all_records())
    fleet_driver_data=fleet_driver_data[['employee_id','name']]
    fleet_driver_data.rename({'employee_id':'ETM','name':'Pilot Name'},axis=1,inplace=True)

    #merging
    all_data=penalty_form_repair_penalty_amount_data.merge(fleet_driver_data,on='ETM',how='left')
    all_data.rename({'Panel name':'Remark','DM':'Team Name'},axis=1,inplace=True)
    all_data['Name of DM']='Repairs'
    all_data['Closure Time']=''
    start_date='2022-07-25'
    all_data=all_data[(all_data['Timestamp'] >= (start_date))]
    all_data=all_data[['Timestamp','ETM','Team Name','Pilot Name','Car Number','Name of DM','Closure Time','Amount','Remark']]

    #concating all data and penalty final 
    penalty_final_df=pd.concat([penalty_final, all_data], ignore_index = True)
    penalty_final_df['Timestamp']=pd.to_datetime(penalty_final_df['Timestamp'])
    penalty_final_df.sort_values(by='Timestamp',ascending=True,inplace=True)
    penalty_final_df


    penalty_gs = gc.open("Penalty Sheet")

    car_recovery_penalty_tab = penalty_gs.worksheet_by_title(
        "Car Recovery Penalty")


    car_recovery_penalty_tab.set_dataframe(penalty_final_df, 'A1')
    
except Exception: 
    sendMail(["abusalameverestfleet@gmail.com",
              "avinash.everestfleet@gmail.com",
              "rutvijaeverestfleet@gmail.com",
              "karan.saraogi@everestfleet.com",
              "sameer.everestfleet@gmail.com"])
print("car_recovery_penalty updated succesfully")