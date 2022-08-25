#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")

#clients

clients = pygsheets.authorize(service_file='client_secret.json')

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)

#Penalty

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('Penalty')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,1,2,7,5]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=56)
df['Date of Fine']= df['Date of Fine'].str.replace('/', '-')
df['Date in Payout']= df['Date in Payout'].str.replace('/', '-')
df[['Date of Fine','Date in Payout']] = df[['Date of Fine','Date in Payout']].apply(pd.to_datetime, errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date of Fine'] >= pd.to_datetime(previous_monday_week)) & (df['Date of Fine'] < pd.to_datetime(previous_monday))]
df_5week['Date of Fine']=df_5week['Date of Fine'].astype(str)
df_5week['Date in Payout']=df_5week['Date in Payout'].astype(str)
#Aatmanirbhar -3
# sheet= client.open('Aatmanirbhar -3')
# ws= sheet.worksheet('Penalty')
# ws.clear()
# ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('Penalty')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('Penalty')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('Penalty')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
# #Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('Penalty')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('Penalty')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('Penalty')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
print("Penalty updated successfully")

#B2B

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('B2B')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[1,3,12,4]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=56)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(previous_monday))]
df_5week['Date']=df_5week['Date'].astype(str)
#Aatmanirbhar -3
# sheet= client.open('Aatmanirbhar -3')
# ws= sheet.worksheet('B2B')
# ws.clear()
# ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('B2B')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('B2B')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('B2B')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('B2B')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('B2B')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('B2B')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
print("B2B updated successfully")

#Recovery

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('Recovery')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,2,5,6]]
today = datetime.date.today()
previous_monday_week= today - datetime.timedelta(days=56)
df['Date']= df['Date'].str.replace('-', '/')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d/%m/%Y')#27/12/2021
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(today))]
df_5week['Date']=df_5week['Date'].astype(str)
#Recovery not in Aatmanirbhar -3 
# sheet= client.open('Aatmanirbhar -3')
# ws= sheet.worksheet('Recovery')
# ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('Recovery')
ws.clear(start='A',end='D')
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('Recovery')
ws.clear(start='A',end='D')
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('Recovery')
ws.clear(start='A',end='D')
ws.set_dataframe(df_5week,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('Recovery')
ws.clear(start='A',end='D')
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('Recovery')
ws.clear(start='A',end='D')
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('Recovery')
ws.clear(start='A',end='D')
ws.set_dataframe(df_5week,(1,1))
print("Recovery updated successfully")

#Adjustments

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('Adjustments')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,1,5,4,7,3]]
today = datetime.date.today()
previous_monday_week= today - datetime.timedelta(days=56)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')#03/06/2022
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] <= pd.to_datetime(today))]
df_5week['Date']=df_5week['Date'].astype(str)
#Aatmanirbhar -3
sheet= clients.open('Aatmanirbhar -3')
ws= sheet.worksheet_by_title('Adjustments')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('Adjustments')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('Adjustments')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('Adjustments')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('Adjustments')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('Adjustments')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('Adjustments') 
ws.clear()
ws.set_dataframe(df_5week,(1,1))
print("Adjustment updated successfully")

#RTO

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('RTO')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,1,2,6,4]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=56)
df['Date of Fine']= df['Date of Fine'].str.replace('/', '-')
df['Date in Payout']= df['Date in Payout'].str.replace('/', '-')
df[['Date of Fine','Date in Payout']] = df[['Date of Fine','Date in Payout']].apply(pd.to_datetime, errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date of Fine'] >= pd.to_datetime(previous_monday_week)) & (df['Date of Fine'] < pd.to_datetime(previous_monday))]
df_5week['Date of Fine']=df_5week['Date of Fine'].astype(str)
df_5week['Date in Payout']=df_5week['Date in Payout'].astype(str)
#Aatmanirbhar -3
# sheet= client.open('Aatmanirbhar -3')
# ws= sheet.worksheet('RTO')
# ws.clear()
# ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('RTO')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('RTO')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('RTO')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('RTO')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('RTO')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('RTO')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
print("Rto updated successfully")

#Driver hisaab

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('Driver Hisaab')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[1,2,3,4,5]]
#Aatmanirbhar -3
sheet= clients.open('Aatmanirbhar -3')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('Driver Hisaab')
ws.clear()
ws.set_dataframe(df,(1,1))
print("Driver hisaab updated successfully")

#Amount paid

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('Amount Paid')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[1,2,5]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=56)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')#27/12/2021
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(previous_monday))]
df_5week['Date']=df_5week['Date'].astype(str)
#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('Amount Paid')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('Amount Paid')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('Amount Paid')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('Amount Paid')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('Amount Paid')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('Amount Paid')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
print("Amount paid updated successfully")

#New sms data

sheet= clients.open('Car Master Sheet')
ws= sheet.worksheet_by_title('New SMS Data')
data = ws.get_all_values()
headers = data.pop(1)
dfm = pd.DataFrame(data,columns=headers)
reject_df=dfm.loc[dfm['Sum of Trips'] > '0']
df=reject_df.iloc[:,[2,3,4,26,31,32,0,1,36]]

car_no_ms_ls='11D8_6u4ywy3yNYyrnMonyti6eflqUtpMfsxgLMBXEzk'

sheet= clients.open_by_key(car_no_ms_ls)#car no master list comparison car
ws= sheet.worksheet_by_title('Current')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
car_number_matching=dfm.iloc[:,[0]]
car_number_matching.rename({'car_number':'vehicle no'},axis=1,inplace=True)
new_sms_filter_data_df=df.merge(car_number_matching,on='vehicle no',how='inner')#comparing the car no is present in car no master or not in new sms data 
new_sms_filter_data=new_sms_filter_data_df.iloc[:,[0,1,2,3,4,5,6,7,8]]
new_sms_filter_data

#Aatmanirbhar -3
sheet= clients.open('Aatmanirbhar -3')
ws= sheet.worksheet_by_title('New SMS Data')
ws.clear(start='A',end='I')
ws.set_dataframe(new_sms_filter_data,(1,1))
print("New sms data updated successfully")

#Allotment status report

# scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
# client= gspread.authorize(credentials)
# sheet= client.open('Allotment Status Report')
# ws= sheet.worksheet('Allotment Status Report')
# data = ws.get_all_values()
# headers = data.pop(0)
# dfm = pd.DataFrame(data,columns=headers)
# df=dfm.iloc[:,[2,16,7,3]] #Dm comparing etm with dm
# allot=df[df.ID.str.startswith('ETM')]
# #Aatmanirbhar -3
# sheet= client.open('Aatmanirbhar -3')
# ws= sheet.worksheet('Allotment Status Report')
# ws.update([allot.columns.values.tolist()] + allot.values.tolist(),value_input_option='USER_ENTERED')
# print("Allotment status report updated successfully")

#uber

sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Uber')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,1,5,4,6,8,12,13,14,15,16,21,23,24,11,19,9,10,26,27,28,29]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=56)
df['Week start Date']= df['Week start Date'].str.replace('/', '-')
df['Week End Date']= df['Week End Date'].str.replace('/', '-')
df[['Week start Date','Week End Date']] = df[['Week start Date','Week End Date']].apply(pd.to_datetime, errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Week start Date'] >= pd.to_datetime(previous_monday_week)) & (df['Week start Date'] < pd.to_datetime(previous_monday))]
df_5week['Week start Date']=df_5week['Week start Date'].astype(str)
df_5week['Week End Date']=df_5week['Week End Date'].astype(str)
# not to update Uber in 26/05/2022 Aatmanirbhar -3 
# sheet= client.open('Aatmanirbhar -3')
# ws= sheet.worksheet('Uber')
# ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#Deep Hunters - Hisaab Tab
sheet= clients.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Roaring Lions - Hisaab Tab
sheet= clients.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Black Panthers - Hisaab Tab
sheet= clients.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Silent Killers - Hisaab Tab
sheet= clients.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Terrific Tigers - Hisaab Tab
sheet= clients.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
#Attrition Team - Hisaab Tab
sheet= clients.open('Attrition Team - Hisaab Tab')
ws= sheet.worksheet_by_title('Uber')
ws.clear()
ws.set_dataframe(df_5week,(1,1))
print("Uber updated successfully")

#vistor response

sheet= clients.open('New Visitors Form at Korum Mall,Thane ')
ws= sheet.worksheet_by_title('Vistor Responses')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[0,5]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=56)
df['Timestamp']= df['Timestamp'].str.replace('/', '-')
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce',format='%d-%m-%Y %H:%M:%S')#27/12/2021
df_5week = df[(df['Timestamp'] >= pd.to_datetime(previous_monday_week)) & (df['Timestamp'] < pd.to_datetime(previous_monday))]
df_5week['Timestamp']=df_5week['Timestamp'].astype(str)
#Aatmanirbhar -3
sheet= clients.open('Aatmanirbhar -3')
ws= sheet.worksheet_by_title('Vistor Responses')
ws.clear(start='A',end='B')
ws.set_dataframe(df_5week,(1,1))
print("Vistor responses updated successfully")

#DM

sheet= clients.open('Car No Master list')
ws= sheet.worksheet_by_title('Current')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
#Aatmanirbhar -3
sheet= clients.open('Aatmanirbhar -3')
ws= sheet.worksheet_by_title('DM')
ws.clear()
ws.set_dataframe(dfm,(1,1))
print("Dm updated successfully")
print("\n")

print('All sheets updated successfully')