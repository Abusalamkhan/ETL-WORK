#importing Libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")

#Leasing Dashboard Korum Mall 

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('New Leasing Master')
ws= sheet.worksheet('Allocation Responses')
data = ws.get_all_values()
sheet= client.open('Leasing Dashboard')
ws= sheet.worksheet('Live Data')
ws.update(data)
print("Leasing Dashboard Korum Mall Live Data Succesfully")

#Leasing Dashboard Chunabhatti 

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Leasing Master - Vinay Bhai')
ws= sheet.worksheet('Allocation Responses')
data = ws.get_all_values()
sheet= client.open('Leasing Dashboard - Vinay Bhai')
ws= sheet.worksheet('Live Data')
ws.update(data)
print("Leasing Dashboard Chunabhatti Live Data Updated Succesfully")