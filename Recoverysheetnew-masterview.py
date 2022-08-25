#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
import warnings
import numpy as np
warnings.filterwarnings("ignore")

#creating connections

#scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
#credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
#client= gspread.authorize(credentials)

clients = pygsheets.authorize(service_file='car-master-sheet.json')

Recoverysheetnew='1j9hmPHNmfjJp7nn4p8EVq8aciL8NfiO3hYWdf28aDzQ'
sheet= clients.open_by_key(Recoverysheetnew)
ws= sheet.worksheet_by_title('Recovery Tabs')
data = ws.get_all_values()
headers=data.pop(0)
recovery_tabs = pd.DataFrame(data,columns=headers)
tabs=list(recovery_tabs['Recovery Tabs'].replace('',np.nan).dropna())
tabs
# dropna=recovery_tabs.str.replace('','NaN')
# r=dropna.dropna('NaN')
# r
# tabs=list(recovery_tabs['Recovery Tabs'])
# tabs
dfs=pd.DataFrame()
for i in tabs:
    if i in tabs:
        sheet= clients.open_by_key(Recoverysheetnew)
        ws= sheet.worksheet_by_title(i)
        data = ws.get_all_values()
        df = pd.DataFrame(data)
        dfs = dfs.append(df,ignore_index = True)
        print(i)
    else:
        print("tab no found")
master_df=dfs.rename(columns=dfs.iloc[0]).drop(dfs.index[0]).reset_index(drop=True)
today = datetime.date.today()
previous_monday = today - datetime.timedelta(days=today.weekday())
master_df['Week Begin date'] = pd.to_datetime(master_df['Week Begin date'], errors='coerce',format='%d/%m/%Y')
df_1week = master_df[(master_df['Week Begin date'] >= pd.to_datetime(previous_monday)) & (master_df['Week Begin date'] <= pd.to_datetime(today))]
df_1week['Week Begin date']=df_1week['Week Begin date'].astype(str)
df_1week = df_1week.dropna(axis=1)
df_1week=df_1week[['Week Begin date','Date of Updating','Date  of Payment','Collection of Purpose','Mode Of Payment','Cash/Online','Name of Employee','Employee ID','Amount','Mobile','DM Name', 'transferred to Name of Person', 'Bank_Transaction_ID','Order_ID']]
df_1week
sheet= clients.open_by_key(Recoverysheetnew)
ws= sheet.worksheet_by_title('Masterview')
#ws.batch_clear(['A:N'])
ws.clear(start='A',end='N')
#ws.update([df_1week.columns.values.tolist()] + df_1week.values.tolist(),value_input_option='USER_ENTERED')
ws.set_dataframe(df_1week,(1,1))
print("masterview updated succesfully")