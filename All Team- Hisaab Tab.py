#Importing all libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")

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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('Uber')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('Uber')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('Uber')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('Uber')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('Uber')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#penalty

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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('Penalty')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('Penalty')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('Penalty')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('Penalty')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('B2B')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('B2B')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('B2B')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('B2B')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('B2B')
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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('RTO')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('RTO')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('RTO')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('RTO')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('RTO')
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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('Adjustments')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('Adjustments')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('Adjustments')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('Adjustments')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('Adjustments')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#Amount Paid

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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('Amount Paid')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('Amount Paid')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('Amount Paid')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('Amount Paid')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('Amount Paid')
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
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('Recovery')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('Recovery')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('Recovery')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('Recovery')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('Recovery')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

#Driver Hisaab

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Car Master Sheet')
ws= sheet.worksheet('Driver Hisaab')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df_5week=dfm.iloc[0:-1,[1,2,3,4,5]]
#Deep Hunters - Hisaab Tab
sheet= client.open('Deep Hunters - Hisaab Tab')
ws= sheet.worksheet('Driver Hisaab')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Roaring Lions - Hisaab Tab
sheet= client.open('Roaring Lions - Hisaab Tab')
ws= sheet.worksheet('Driver Hisaab')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Black Panthers - Hisaab Tab
sheet= client.open('Black Panthers - Hisaab Tab')
ws= sheet.worksheet('Driver Hisaab')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Silent Killers - Hisaab Tab
sheet= client.open('Silent Killers - Hisaab Tab')
ws= sheet.worksheet('Driver Hisaab')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')
#Terrific Tigers - Hisaab Tab
sheet= client.open('Terrific Tigers - Hisaab Tab')
ws= sheet.worksheet('Driver Hisaab')
ws.update([df_5week.columns.values.tolist()] + df_5week.values.tolist(),value_input_option='USER_ENTERED')

print("Updated Succesfully")