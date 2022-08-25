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
sheet= client.open_by_key('1cpR6AVVpk9TF4_I38IFYPPOqk-_bSROHgVVYdaXLXOI')#Allotment Status Report
ws= sheet.worksheet('New Allotment History')
data = ws.get_all_values()
headers = data.pop(0)
dfm = pd.DataFrame(data,columns=headers)
df=dfm.iloc[:,[8,2,3,4,5,7,9,10]]
df['Timestamp']= df['Timestamp'].str.replace('-', '/')
df['Allotment Date']= df['Allotment Date'].str.replace('-', '/')
df['Return Date']= df['Return Date'].str.replace('-', '/')
df[['Timestamp','Allotment Date','Return Date']] = df[['Timestamp','Allotment Date','Return Date']].apply(pd.to_datetime, errors='coerce',format='%d/%m/%Y %H:%M:%S')
df.sort_values(by='Timestamp',ascending=False, inplace=True)
df['Timestamp']=df['Timestamp'].astype(str)
df[['Allotment Date','Return Date']]=df[['Allotment Date','Return Date']].astype(str).replace('NaT', '')
sheet= client.open_by_key('1dhSoYdAx8hI09ztG6M5thfWuq7eOgN5SqaZ_CvQlMTA')#live attrition 2.0
ws= sheet.worksheet('Allotment Query data')
ws.update([df.columns.values.tolist()] + df.values.tolist(),value_input_option='USER_ENTERED')
print("Live attrition 2.0 automation updated succesfully")