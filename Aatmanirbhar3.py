#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")

#Penalty

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Penalty')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[0,1,2,7,5]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Date of Fine']= df['Date of Fine'].str.replace('/', '-')
df['Date in Payout']= df['Date in Payout'].str.replace('/', '-')
df[['Date of Fine','Date in Payout']] = df[['Date of Fine','Date in Payout']].apply(pd.to_datetime, errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date of Fine'] >= pd.to_datetime(previous_monday_week)) & (df['Date of Fine'] < pd.to_datetime(previous_monday))]
df_5week['Date of Fine']=df_5week['Date of Fine'].astype(str)
df_5week['Date in Payout']=df_5week['Date in Payout'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Penalty')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#B2B

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('B2B')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[1,3,12,4]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(previous_monday))]
df_5week['Date']=df_5week['Date'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('B2B')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#Recovery

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Recovery')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[0,2,5,6]]
today = datetime.date.today()
# previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Date']= df['Date'].str.replace('-', '/')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d/%m/%Y')#27/12/2021
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(today))]
df_5week['Date']=df_5week['Date'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Recovery')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#Adjustments

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Adjustments')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[0,1,5,4,7,3]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(previous_monday))]
df_5week['Date']=df_5week['Date'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Adjustments')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#RTO

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('RTO')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[0,1,2,6,4]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Date of Fine']= df['Date of Fine'].str.replace('/', '-')
df['Date in Payout']= df['Date in Payout'].str.replace('/', '-')
df[['Date of Fine','Date in Payout']] = df[['Date of Fine','Date in Payout']].apply(pd.to_datetime, errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Date of Fine'] >= pd.to_datetime(previous_monday_week)) & (df['Date of Fine'] < pd.to_datetime(previous_monday))]
df_5week['Date of Fine']=df_5week['Date of Fine'].astype(str)
df_5week['Date in Payout']=df_5week['Date in Payout'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('RTO')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#Driver hisaab

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Driver Hisaab')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[1,2,3,4,5]]
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Driver Hisaab')
ws.update([df.columns.values.tolist()] + df.values.tolist(),value_input_option='USER_ENTERED')

#Amount paid

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Amount Paid')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[1,2,5]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Date']= df['Date'].str.replace('/', '-')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce',format='%d-%m-%Y')#27/12/2021
df_5week = df[(df['Date'] >= pd.to_datetime(previous_monday_week)) & (df['Date'] < pd.to_datetime(previous_monday))]
df_5week['Date']=df_5week['Date'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Amount Paid')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#New sms data

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('New SMS Data')
data = ws.get_all_values()
headers = data.pop(1)
dfm = pd.DataFrame(data,columns=headers)
a=dfm.loc[:,['Contact No','Name','ETM','Total Earnings(Sum(Q:S)-T)','Pending Joining Fee']]
b=dfm.iloc[0:-1,[32]]
df= pd.concat([a, b], axis=1, join="inner")
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('New SMS Data')
ws.update([df.columns.values.tolist()] + df.values.tolist(),value_input_option='USER_ENTERED')

#Allotment status report

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Allotment Status Report')
ws= sheet.worksheet('Allotment Status Report')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[2,16,7,3]] #Dm comparing etm with dm
allot=df[df.ID.str.startswith('ETM')]
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Allotment Status Report')
ws.update([allot.columns.values.tolist()] + allot.values.tolist(),value_input_option='USER_ENTERED')

#uber

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Uber')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[0,1,5,4,6,8,12,13,14,15,16,21,23,24,11,19,9,10,26,27,28,29]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Week start Date']= df['Week start Date'].str.replace('/', '-')
df['Week End Date']= df['Week End Date'].str.replace('/', '-')
df[['Week start Date','Week End Date']] = df[['Week start Date','Week End Date']].apply(pd.to_datetime, errors='coerce',format='%d-%m-%Y')
df_5week = df[(df['Week start Date'] >= pd.to_datetime(previous_monday_week)) & (df['Week start Date'] < pd.to_datetime(previous_monday))]
df_5week['Week start Date']=df_5week['Week start Date'].astype(str)
df_5week['Week End Date']=df_5week['Week End Date'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Uber')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#vistor response

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('New Visitors Form at Korum Mall,Thane ')
ws= sheet.worksheet('Vistor Responses')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[0:-1,[0,5]]
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
previous_monday_week= previous_monday - datetime.timedelta(days=35)
df['Timestamp']= df['Timestamp'].str.replace('/', '-')
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce',format='%d-%m-%Y %H:%M:%S')#27/12/2021
df_5week = df[(df['Timestamp'] >= pd.to_datetime(previous_monday_week)) & (df['Timestamp'] < pd.to_datetime(previous_monday))]
df_5week['Timestamp']=df_5week['Timestamp'].astype(str)
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('Vistor Responses')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#DM

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car No Master list ')
ws= sheet.worksheet('Current')
data = ws.get_all_values()
sheet= client.open('Aatmanirbhar -3')
ws= sheet.worksheet('DM')
ws.update(data)

print('updated successfully')