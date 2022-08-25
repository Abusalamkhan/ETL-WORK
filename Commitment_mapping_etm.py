import pygsheets #Importing python in google sheets
import pandas as pd #importing pandas
from pandas import Series,DataFrame
import datetime as dt
from datetime import datetime, timedelta,date
import numpy as np


# ETM	Pilot Name	Mobile Number	alternate_number	

# Rating	Last Call	Todays Call	Yesterday Call Final OS


my_date = date.today() 

my_time = datetime.min.time()
my_datetime = datetime.combine(my_date, my_time)

week_start = my_date - timedelta(days=my_date.weekday())

week_end = week_start - timedelta(days=7)

week_end_datetime = datetime.combine(week_end, my_time)

today = my_datetime
yesterday = today - timedelta(days = 1)

tomorrow = today + timedelta(days = 1)

gc = pygsheets.authorize(service_file='client_secret.json')

car_status_report_sheet = gc.open("Car Status Report")

car_allotment_tab = car_status_report_sheet.worksheet_by_title("Cars")

car_allotment_data = pd.DataFrame(car_allotment_tab.get_all_records())


car_allotment_data = car_allotment_data[['Car Number','ETM']]

car_allotment_data['ETM']=car_allotment_data['ETM'].str.strip()

car_allotment_data['ETM']=car_allotment_data['ETM'].replace('-','unalloted')

car_allotment_data = car_allotment_data.groupby(['Car Number']).max()

car_allotment_data.reset_index(inplace = True)


fleet_driver_sheet = gc.open("Fleet_driver")

fleet_driver_tab = fleet_driver_sheet.worksheet_by_title("Fleet_driver")

fleet_driver = pd.DataFrame(fleet_driver_tab.get_all_records())

fleet_driver = fleet_driver[['employee_id','name','mobile','alternate_number']]


fleet_driver.rename(columns={"employee_id":"ETM"},inplace = True)


final_table = pd.merge(car_allotment_data,fleet_driver,on='ETM',how='left')

print(final_table.head())

commitment_mapping_master_sheet = gc.open("Commitment Mapping 3.0")

commitment_mapping_calling_tab = commitment_mapping_master_sheet.worksheet_by_title("Calling Data")

commitment_mapping_calling_data = pd.DataFrame(commitment_mapping_calling_tab.get_all_records())

commitment_mapping_calling_data = commitment_mapping_calling_data[['Timestamp','ETM ID','Driver Status?']]

commitment_mapping_calling_data.rename(columns={"ETM ID":"ETM"},inplace = True)

commitment_mapping_calling_data['Timestamp'] = pd.to_datetime(commitment_mapping_calling_data['Timestamp'])

max_call_data = commitment_mapping_calling_data[['Timestamp','ETM']]


max_call = max_call_data.groupby(['ETM']).max()


yesterday_commitment_calling_data = commitment_mapping_calling_data.copy()
today_commitment_calling_data = commitment_mapping_calling_data.copy()




today_commitment_calling_data['start date'] = today
today_commitment_calling_data['end date'] = tomorrow

print(today_commitment_calling_data.tail())



print(today_commitment_calling_data.tail())

today_commitment_calling_data = today_commitment_calling_data.loc[(today_commitment_calling_data['start date'] < today_commitment_calling_data['Timestamp']) & (today_commitment_calling_data['end date'] > today_commitment_calling_data['Timestamp'])]


print(today_commitment_calling_data.tail())




yesterday_commitment_calling_data['start date'] = yesterday
yesterday_commitment_calling_data['end date'] = today



yesterday_commitment_calling_data = yesterday_commitment_calling_data.loc[(yesterday_commitment_calling_data['start date'] < yesterday_commitment_calling_data['Timestamp']) & (yesterday_commitment_calling_data['end date'] > yesterday_commitment_calling_data['Timestamp'])]

print(yesterday_commitment_calling_data.head())

today_commitment_calling_data = today_commitment_calling_data[['ETM','Driver Status?']]

today_commitment_calling_data.rename(columns={"Driver Status?":"Today Call"},inplace = True)


today_call = today_commitment_calling_data.groupby(['ETM']).max()

yesterday_commitment_calling_data = yesterday_commitment_calling_data[['ETM','Driver Status?']]

yesterday_commitment_calling_data.rename(columns={"Driver Status?":"Yesterday Call"},inplace = True)

yesterday_call = yesterday_commitment_calling_data.groupby(['ETM']).max()


final_table = pd.merge(final_table,max_call,on='ETM',how='left')
final_table = pd.merge(final_table,today_call,on='ETM',how='left')
final_table = pd.merge(final_table,yesterday_call,on='ETM',how='left')

print(final_table.head())




cms_sheet = gc.open("Car Master Sheet")

hisaab_tab = cms_sheet.worksheet_by_title("Driver Hisaab Final")

hisaab_data = pd.DataFrame(hisaab_tab.get_all_records())

hisaab_data = hisaab_data[['Driver ETM','Final OS']]



hisaab_data.rename(columns={"Driver ETM":"ETM"},inplace = True)



final_table = pd.merge(final_table,hisaab_data,on='ETM',how='left')


asr_mumbai_sheet = gc.open("Allotment Status Report")

asr_mumbai = asr_mumbai_sheet.worksheet_by_title("Allotment Status Report")

allotment_mumbai = pd.DataFrame(asr_mumbai.get_all_records())

allotment_mumbai = allotment_mumbai[['Active or not','Timestamp','ID']]

allotment_mumbai['Timestamp'] = pd.to_datetime(allotment_mumbai['Timestamp'],dayfirst = True)

allotment_mumbai.rename(columns={"ID":"ETM"},inplace = True)


allotment_mumbai = allotment_mumbai.loc[allotment_mumbai['Active or not'] == 'Active']
allotment_mumbai = allotment_mumbai[['Timestamp','ETM']]
first_allotment = allotment_mumbai.groupby(['ETM']).min()
print(first_allotment.head())
first_allotment['end_date'] = week_end_datetime
first_allotment = first_allotment.loc[(first_allotment['end_date'] < first_allotment['Timestamp'])]
first_allotment['Rating'] = 'New'
first_allotment.drop(['Timestamp','end_date'],axis = 1, inplace = True)
print(first_allotment.head())
last_allotment = allotment_mumbai.groupby('ETM').max()
last_allotment.rename(columns={"Timestamp":"Last Allotment Date"},inplace = True)
print(last_allotment.head())
final_table = pd.merge(final_table,first_allotment,on='ETM',how='left')
final_table = pd.merge(final_table,last_allotment,on='ETM',how='left')
print(final_table.head())
final_table["Final OS"].fillna(0,inplace = True)
final_table.fillna("",inplace = True)
final_table.rename(columns={"Car Number":"car_number","Timestamp":"Last Call Time"},inplace = True)


#Commitment Mapping 3.0
commitment_mapping_master_sheet = gc.open("Commitment Mapping 3.0")
commitment_mapping_master_tab = commitment_mapping_master_sheet.worksheet_by_title("Master")
commitment_mapping_master = pd.DataFrame(commitment_mapping_master_tab.get_all_records())
commitment_mapping_master = commitment_mapping_master[['car_number','Last Week Trips']]
commitmentmapping_table = pd.merge(commitment_mapping_master,final_table,on='car_number',how='left')

#rating piker laggard calculation.

commitmentmapping_table['Last Week Trips']=commitmentmapping_table['Last Week Trips'].replace(np.nan,0).replace('',0)
commitmentmapping_table['Last Week Trips'] = commitmentmapping_table['Last Week Trips'].astype(float)
aa= []
for i in commitmentmapping_table.index:
    if commitmentmapping_table['Rating'].values[i]=='New':
        aa.append('New')
    elif commitmentmapping_table['Last Week Trips'].values[i] == 0:
        aa.append('ND')
    elif commitmentmapping_table['Last Week Trips'].values[i] < 30.0:
        aa.append('Piker')
    elif commitmentmapping_table['Last Week Trips'].values[i] < 55.0:
        aa.append('Laggard')
    elif commitmentmapping_table['Last Week Trips'].values[i] < 75.0:
        aa.append('Mediocre') 
    elif commitmentmapping_table['Last Week Trips'].values[i] < 95.0:
        aa.append('Performer')
    elif commitmentmapping_table['Last Week Trips'].values[i] >= 95.0:
        aa.append('out Performer')
    else:
        aa.append('')
commitmentmapping_table['Rating']=aa[0:commitmentmapping_table.shape[0]]
commitmentmapping_table


commitmentmapping_table['car_number'] = commitmentmapping_table['car_number'].replace(r'^\s*$', np.nan, regex=True)
commitmentmapping_table = commitmentmapping_table[commitmentmapping_table['car_number'].notna()]

ETM_table = commitmentmapping_table[['ETM','name','mobile','alternate_number','Rating','Last Call Time','Today Call','Yesterday Call']]
final_OS_table = commitmentmapping_table[['Final OS']]
last_allotment = commitmentmapping_table[['Last Allotment Date']]
commitment_mapping_master_tab.set_dataframe(ETM_table,'E1')
commitment_mapping_master_tab.set_dataframe(final_OS_table,'V1')
commitment_mapping_master_tab.set_dataframe(last_allotment,'Z1')

#roarings
commitment_mapping_master_sheet = gc.open("Commitment Mapping - Roaring Lions")
commitment_mapping_master_tab = commitment_mapping_master_sheet.worksheet_by_title("Roaring_Lions")
commitment_mapping_master = pd.DataFrame(commitment_mapping_master_tab.get_all_records())
commitment_mapping_master = commitment_mapping_master[['car_number']]
commitment_mapping_master['car_number'] = commitment_mapping_master['car_number'].replace(r'^\s*$', np.nan, regex=True)
commitment_mapping_master = commitment_mapping_master[commitment_mapping_master['car_number'].notna()]
commitmentmapping_table = pd.merge(commitment_mapping_master,final_table,on='car_number',how='left')

ETM_table = commitmentmapping_table[['ETM','name','mobile','alternate_number','Rating','Last Call Time','Today Call','Yesterday Call']]
final_OS_table = commitmentmapping_table[['Final OS']]
last_allotment = commitmentmapping_table[['Last Allotment Date']]
commitment_mapping_master_tab.set_dataframe(ETM_table,'E1')
commitment_mapping_master_tab.set_dataframe(final_OS_table,'V1')
commitment_mapping_master_tab.set_dataframe(last_allotment,'Z1')

#terrific
commitment_mapping_master_sheet = gc.open("Commitment Mapping - Terrific Tigers")
commitment_mapping_master_tab = commitment_mapping_master_sheet.worksheet_by_title("Terrific_Tigers")
commitment_mapping_master = pd.DataFrame(commitment_mapping_master_tab.get_all_records())
commitment_mapping_master = commitment_mapping_master[['car_number']]
commitment_mapping_master['car_number'] = commitment_mapping_master['car_number'].replace(r'^\s*$', np.nan, regex=True)
commitment_mapping_master = commitment_mapping_master[commitment_mapping_master['car_number'].notna()]
commitmentmapping_table = pd.merge(commitment_mapping_master,final_table,on='car_number',how='left')

ETM_table = commitmentmapping_table[['ETM','name','mobile','alternate_number','Rating','Last Call Time','Today Call','Yesterday Call']]
final_OS_table = commitmentmapping_table[['Final OS']]
last_allotment = commitmentmapping_table[['Last Allotment Date']]
commitment_mapping_master_tab.set_dataframe(ETM_table,'E1')
commitment_mapping_master_tab.set_dataframe(final_OS_table,'V1')
commitment_mapping_master_tab.set_dataframe(last_allotment,'Z1')

#black
commitment_mapping_master_sheet = gc.open("Commitment Mapping - Black Panthers")
commitment_mapping_master_tab = commitment_mapping_master_sheet.worksheet_by_title("Black_Panthers")
commitment_mapping_master = pd.DataFrame(commitment_mapping_master_tab.get_all_records())
commitment_mapping_master = commitment_mapping_master[['car_number']]
commitment_mapping_master['car_number'] = commitment_mapping_master['car_number'].replace(r'^\s*$', np.nan, regex=True)
commitment_mapping_master = commitment_mapping_master[commitment_mapping_master['car_number'].notna()]
commitmentmapping_table = pd.merge(commitment_mapping_master,final_table,on='car_number',how='left')

ETM_table = commitmentmapping_table[['ETM','name','mobile','alternate_number','Rating','Last Call Time','Today Call','Yesterday Call']]
final_OS_table = commitmentmapping_table[['Final OS']]
last_allotment = commitmentmapping_table[['Last Allotment Date']]
commitment_mapping_master_tab.set_dataframe(ETM_table,'E1')
commitment_mapping_master_tab.set_dataframe(final_OS_table,'V1')
commitment_mapping_master_tab.set_dataframe(last_allotment,'Z1')

#silent
commitment_mapping_master_sheet = gc.open("Commitment Mapping - Silent Killers")
commitment_mapping_master_tab = commitment_mapping_master_sheet.worksheet_by_title("Silent_Killers")
commitment_mapping_master = pd.DataFrame(commitment_mapping_master_tab.get_all_records())
commitment_mapping_master = commitment_mapping_master[['car_number']]
commitment_mapping_master['car_number'] = commitment_mapping_master['car_number'].replace(r'^\s*$', np.nan, regex=True)
commitment_mapping_master = commitment_mapping_master[commitment_mapping_master['car_number'].notna()]
commitmentmapping_table = pd.merge(commitment_mapping_master,final_table,on='car_number',how='left')

ETM_table = commitmentmapping_table[['ETM','name','mobile','alternate_number','Rating','Last Call Time','Today Call','Yesterday Call']]
final_OS_table = commitmentmapping_table[['Final OS']]
last_allotment = commitmentmapping_table[['Last Allotment Date']]
commitment_mapping_master_tab.set_dataframe(ETM_table,'E1')
commitment_mapping_master_tab.set_dataframe(final_OS_table,'V1')
commitment_mapping_master_tab.set_dataframe(last_allotment,'Z1')

#deep
commitment_mapping_master_sheet = gc.open("Commitment Mapping - Deep Hunters")
commitment_mapping_master_tab = commitment_mapping_master_sheet.worksheet_by_title("Deep_Hunters")
commitment_mapping_master = pd.DataFrame(commitment_mapping_master_tab.get_all_records())
commitment_mapping_master = commitment_mapping_master[['car_number']]
commitment_mapping_master['car_number'] = commitment_mapping_master['car_number'].replace(r'^\s*$', np.nan, regex=True)
commitment_mapping_master = commitment_mapping_master[commitment_mapping_master['car_number'].notna()]
commitmentmapping_table = pd.merge(commitment_mapping_master,final_table,on='car_number',how='left')

ETM_table = commitmentmapping_table[['ETM','name','mobile','alternate_number','Rating','Last Call Time','Today Call','Yesterday Call']]
final_OS_table = commitmentmapping_table[['Final OS']]
last_allotment = commitmentmapping_table[['Last Allotment Date']]
commitment_mapping_master_tab.set_dataframe(ETM_table,'E1')
commitment_mapping_master_tab.set_dataframe(final_OS_table,'V1')
commitment_mapping_master_tab.set_dataframe(last_allotment,'Z1')