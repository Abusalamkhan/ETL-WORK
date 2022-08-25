import pygsheets #Importing python in google sheets
import pandas as pd #importing pandas
from pandas import Series,DataFrame
import datetime as dt
from datetime import datetime, timedelta
import numpy as np


gc = pygsheets.authorize(service_file='client_secret.json')


all_in_one_gs = gc.open("All  in One Form - Mumbai")


i = 0
while i < 30:
    all_in_one_responses_tab = all_in_one_gs[i]
    if all_in_one_responses_tab.title == "Form Responses":
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


penalty_response_dead_km = all_in_one_responses[['Timestamp','ETM','Team Name','Pilot Name','Dead KM/Car Misuse - Please insert only KM','Date of Penalty','Penalty Comment','Car Number','Name of DM']]

penalty_response_dead_km = penalty_response_dead_km.replace(r'^\s*$', np.nan, regex=True)

penalty_response_dead_km = penalty_response_dead_km[penalty_response_dead_km['Dead KM/Car Misuse - Please insert only KM'].notna()]

penalty_response_dead_km = penalty_response_dead_km.replace(np.nan,'',regex=True)


penalty_response_dead_km['Amount'] = penalty_response_dead_km['Dead KM/Car Misuse - Please insert only KM'] * 10

penalty_response_dead_km['Dead KM col'] = 'Dead KM:'

penalty_response_dead_km['Dead KM/Car Misuse - Please insert only KM'] = penalty_response_dead_km['Dead KM/Car Misuse - Please insert only KM'].astype(str)


penalty_response_dead_km['Penalty Reason Final'] = penalty_response_dead_km['Dead KM col'] + penalty_response_dead_km['Dead KM/Car Misuse - Please insert only KM']

del penalty_response_dead_km['Dead KM/Car Misuse - Please insert only KM']

del penalty_response_dead_km['Dead KM col']




penalty_final = pd.concat([penalty_final, penalty_response_dead_km],ignore_index = True)

penalty_final['Timestamp'] = pd.to_datetime(penalty_final['Timestamp'])

penalty_final.sort_values(by=['Timestamp'],ascending = True,inplace = True)

penalty_final = penalty_final[['Timestamp','ETM','Team Name','Pilot Name','Date of Penalty','Penalty Comment','Penalty Reason Final','Amount','Car Number','Name of DM']]




# car_recovery_penalty = all_in_one_responses_copy[['Timestamp','ETM','Team Name','Driver Left','Pilot Name','Key required?','Car Number','Name of DM']]

# car_recovery_penalty['Timestamp'] = pd.to_datetime(car_recovery_penalty['Timestamp'])

# #500 for car recovery
# car_recovery_penalty_copy = car_recovery_penalty.copy()
# car_recovery_penalty_copy = car_recovery_penalty_copy.loc[(car_recovery_penalty_copy['Driver Left'] == 'Car Recovery')]
# car_recovery_penalty_copy['start_date'] = pd.Timestamp('2022-07-05')
# car_recovery_penalty_copy = car_recovery_penalty_copy.loc[(car_recovery_penalty_copy['Timestamp'] > car_recovery_penalty_copy['start_date'])]
# car_recovery_penalty_copy.rename(columns={"Driver Left":"Penalty Reason Final"},inplace = True)
# car_recovery_penalty_copy['Penalty Comment'] = car_recovery_penalty_copy['Penalty Reason Final']
# car_recovery_penalty_copy['Amount'] = 500
# car_recovery_penalty_copy['Date of Penalty'] = car_recovery_penalty_copy['Timestamp'].dt.date
# car_recovery_penalty_copy.drop(['start_date','Key required?'],axis = 1, inplace = True)


# penalty_final = pd.concat([penalty_final, car_recovery_penalty_copy],ignore_index = True)



# #### 850 for key lost
# car_recovery_penalty = car_recovery_penalty.loc[(car_recovery_penalty['Key required?'] == 'Original Key required')]



# car_recovery_penalty['start_date'] = pd.Timestamp('2022-07-05')
# car_recovery_penalty = car_recovery_penalty.loc[(car_recovery_penalty['Timestamp'] > car_recovery_penalty['start_date'])]



# car_recovery_penalty.drop(['start_date','Driver Left'],axis = 1, inplace = True)

# car_recovery_penalty.rename(columns={"Key required?":"Penalty Reason Final"},inplace = True)

# car_recovery_penalty['Penalty Comment'] = car_recovery_penalty['Penalty Reason Final']

# car_recovery_penalty['Amount'] = 850

# car_recovery_penalty['Date of Penalty'] = car_recovery_penalty['Timestamp'].dt.date



# print(car_recovery_penalty.head())



# penalty_final = pd.concat([penalty_final, car_recovery_penalty],ignore_index = True)

# print(penalty_final.tail(10))


# penalty_final.sort_values(by=['Timestamp'],ascending = True,inplace = True)

# print(penalty_final.tail(10))










i = 0
while i < 30:
    penalty_tab = all_in_one_gs[i]
    if penalty_tab.title == "Penalty":
        break
    i +=1
print(penalty_tab.title)

penalty_tab.set_dataframe(penalty_final,'A1')


