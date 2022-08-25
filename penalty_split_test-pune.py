import pygsheets #Importing python in google sheets
import pandas as pd #importing pandas
from pandas import Series,DataFrame
import datetime as dt
from datetime import datetime, timedelta
import numpy as np


gc = pygsheets.authorize(service_file='car-master-sheet.json')


all_in_one_gs = gc.open("All in One Form  - Pune")


i = 0
while i < 30:
    all_in_one_responses_tab = all_in_one_gs[i]
    if all_in_one_responses_tab.title == "Form responses":
        break
    i +=1
print(all_in_one_responses_tab.title)

all_in_one_responses = pd.DataFrame(all_in_one_responses_tab.get_all_records())

all_in_one_responses_copy = all_in_one_responses.copy()

all_in_one_responses = all_in_one_responses.loc[(all_in_one_responses['Go to the Section'] == 'Penalty')]

penalty_responses = all_in_one_responses[['Timestamp','ETM','Team Name','Pilot Name','Penalty Reason','Date of Penalty','Penalty Comment','Car Number','Name of DM']]

penalty_responses = penalty_responses.replace(r'^\s*$', np.nan, regex=True)

penalty_responses = penalty_responses[penalty_responses['Penalty Reason'].notna()]

penalty_responses = penalty_responses.replace(np.nan,'',regex=True)

penalty_responses_new = penalty_responses['Penalty Reason'].str.split(',').apply(Series, 1).stack()

print(penalty_responses_new.head())





penalty_responses_new.index = penalty_responses_new.index.droplevel(-1)

penalty_responses_new.name = 'Penalty Reason'

print(penalty_responses_new.head())

del penalty_responses['Penalty Reason']

penalty_final = penalty_responses.join(penalty_responses_new)

print(penalty_final.head())

penalty_final[['Penalty Reason Final', 'Amount']] = penalty_final['Penalty Reason'].str.split('-', 1, expand=True)

print(penalty_final.head())

del penalty_final['Penalty Reason']

penalty_final['Penalty Reason Final'] = penalty_final['Penalty Reason Final'].str.strip()

print(penalty_final.tail())


penalty_response_dead_km = all_in_one_responses[['Timestamp','ETM','Team Name','Pilot Name','Dead KM','Date of Penalty','Penalty Comment','Car Number','Name of DM']]

penalty_response_dead_km = penalty_response_dead_km.replace(r'^\s*$', np.nan, regex=True)

penalty_response_dead_km = penalty_response_dead_km[penalty_response_dead_km['Dead KM'].notna()]

penalty_response_dead_km = penalty_response_dead_km.replace(np.nan,'',regex=True)


penalty_response_dead_km['Amount'] = penalty_response_dead_km['Dead KM'] * 10

penalty_response_dead_km['Dead KM col'] = 'Dead KM:'

penalty_response_dead_km['Dead KM'] = penalty_response_dead_km['Dead KM'].astype(str)


penalty_response_dead_km['Penalty Reason Final'] = penalty_response_dead_km['Dead KM col'] + penalty_response_dead_km['Dead KM']

del penalty_response_dead_km['Dead KM']

del penalty_response_dead_km['Dead KM col']




penalty_final = pd.concat([penalty_final, penalty_response_dead_km],ignore_index = True)

penalty_final['Timestamp'] = pd.to_datetime(penalty_final['Timestamp'], format='%d/%m/%Y %H:%M:%S')

penalty_final.sort_values(by=['Timestamp'],ascending = True,inplace = True)

penalty_final = penalty_final[['Timestamp','ETM','Team Name','Pilot Name','Date of Penalty','Penalty Comment','Penalty Reason Final','Amount','Car Number','Name of DM']]

print(penalty_final.columns)
print(penalty_final.head())
i = 0
while i < 30:
    penalty_tab = all_in_one_gs[i]
    if penalty_tab.title == "Penalty":
        break
    i +=1
print(penalty_tab.title)

penalty_tab.set_dataframe(penalty_final,'A1')