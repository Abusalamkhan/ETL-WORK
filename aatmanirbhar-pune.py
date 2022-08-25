#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)

car_master_pune='1S9qECmC9EEo4e62Ujby9ATYUjkJbYE36BuvLtJFs_9c'
aatmanirbhar_pune='1-wFeo4oUvXUasGZNw1LN3V-FSOFbOy2j0xIjLIIB1LE'
car_no_master_list_pune='1IBgvI-sJX7ofS_fCn8cM9aS9xQgkG-kZp5nCgHTb9qI'
allotment_status_report_pune='1zjsWVZyeBOGBSvDtwFjIglNiUyHbOhtFNdHnsWtvT-k'

#New sms data

sheet= client.open_by_key(car_master_pune)
ws= sheet.worksheet('New SMS Data')
data = ws.get_all_values()
headers = data.pop(1)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[2,3,4,21,26,27,0,1]]
#Aatmanirbhar pune
sheet= client.open_by_key(aatmanirbhar_pune)
ws= sheet.worksheet('New SMS Data')
ws.batch_clear(['A:H'])
ws.update([df.columns.values.tolist()] + df.values.tolist(),value_input_option='USER_ENTERED')
print("new sms data updated succesfully")

#Adjustment

sheet= client.open_by_key(car_master_pune)
ws= sheet.worksheet('Adjustments')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,1,5,4,7,3]]
today = datetime.date.today()
previous_monday_week= today - datetime.timedelta(days=42)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] <= pd.to_datetime(today))]
df_5week['Date']=df_5week['Date'].astype(str)
#Aatmanirbhar pune
sheet= client.open_by_key(aatmanirbhar_pune)
ws= sheet.worksheet('Adjustments')
ws.batch_clear(['A:F'])
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
print("Adjustment updated successfully")

#Driver hisaab

sheet= client.open_by_key(car_master_pune)
ws= sheet.worksheet('Driver Hisaab')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[1,2,3,4,5]]
#Aatmanirbhar pune
sheet= client.open_by_key(aatmanirbhar_pune)
ws= sheet.worksheet('Driver Hisaab')
ws.batch_clear(['A:E'])
ws.update([df.columns.values.tolist()] + df.values.tolist(),value_input_option='USER_ENTERED')
print("Driver hisaab updated successfully")

#Allotment status report

sheet= client.open_by_key(allotment_status_report_pune)
ws= sheet.worksheet('Allotment Status Report')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[2,13,5,3]] 
#Aatmanirbhar pune
sheet= client.open_by_key(aatmanirbhar_pune)
ws= sheet.worksheet('Allotment Status Report')
ws.batch_clear(['A:D'])
ws.update([df.columns.values.tolist()] + df.values.tolist(),value_input_option='USER_ENTERED')
print("Allotment status report updated successfully")

#DM

sheet= client.open('Car No Master list')
ws= sheet.worksheet('Current')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
pune_team=dfm[dfm['Current DM'].isin(['Terrian Tuskers','Pune Leasing'])]
#Aatmanirbhar pune
sheet= client.open_by_key(aatmanirbhar_pune)
ws= sheet.worksheet('DM')
ws.batch_clear(['A:D'])
ws.update([pune_team.columns.values.tolist()] + pune_team.values.tolist(),value_input_option='USER_ENTERED')
print("DM updated successfully")
print('All sheets updated successfully')