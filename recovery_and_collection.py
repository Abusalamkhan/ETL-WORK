#importing libraries
import pandas as pd
import numpy as np
import pygsheets
import warnings
warnings.filterwarnings("ignore")

#creating connections
clients = pygsheets.authorize(service_file='car-master-sheet.json')

#sheets and code
Recoverysheetnew='1j9hmPHNmfjJp7nn4p8EVq8aciL8NfiO3hYWdf28aDzQ'
newleasingmaster='1D0U02U0QN7pXsgM4AdOUiUKB4rB-SoTwSspWkuwl_8E'
sheet= clients.open_by_key(Recoverysheetnew)
ws= sheet.worksheet_by_title('Recovery Tabs')
data = ws.get_all_values()
headers=data.pop(0)
recovery_tabs = pd.DataFrame(data,columns=headers)
tabs=list(recovery_tabs['Recovery Tabs'].replace('',np.nan).dropna())
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
df=dfs.rename(columns=dfs.iloc[0]).drop(dfs.index[0]).reset_index(drop=True)
a=df.loc[df['Collection of Purpose'].isin(['Mumbai leasing Rent','Mumbai Leasing Deposit'])]
dfm=a.iloc[:,0:14]
dfm.dropna()
sheet= clients.open_by_key(newleasingmaster)
ws= sheet.worksheet_by_title('Recovery')
ws.clear()
ws.set_dataframe(dfm,(1,1))
print("New leasing master- recovery tab updated succesfully")

sheet= clients.open_by_key(newleasingmaster)
ws= sheet.worksheet_by_title('Leasing Payment')
data = ws.get_all_values()
headers=data.pop(0)
leasing_payments = pd.DataFrame(data,columns=headers)
leasing_payment=leasing_payments.loc[leasing_payments['Confirmed?'] == 'Yes']
lsp_df=leasing_payment.iloc[:,[2,1,8,8,4,3,5,6,9]]
cols = []
count = 1
for column in lsp_df.columns:
    if column == 'Types of payment':
        cols.append(f'Types of payment{count}')
        count+=1
        continue
    cols.append(column)
lsp_df.columns = cols
lsp_df.rename({'Date':'Date of payment','Types of payment1': 'Types of payment','Types of payment2':'online'}, axis=1, inplace=True)
lsp_df['online'] = lsp_df['online'].str.capitalize()
lsp_df['online'] = lsp_df['online'].apply(lambda x : 'Cash' if x == 'Cash' else '' if x == '' else 'Online')
lsp_df.rename({'Date of payment':'Date  of Payment','Rent or Deposit?':'Collection of Purpose','Types of payment':'Mode Of Payment','online':'Cash/Online','Name':'Name of Employee','ETM ID':'Employee ID','Phone Number':'Mobile','DM name':'DM Name'}, axis=1, inplace=True)

sheet= clients.open_by_key(newleasingmaster)
ws= sheet.worksheet_by_title('Recovery')
data = ws.get_all_values()
headers=data.pop(0)
recovery = pd.DataFrame(data,columns=headers)
recovery_df=recovery.iloc[:,[0,2,3,4,5,6,7,8,9,10]]
recovery_df['Week Begin date']= recovery_df['Week Begin date'].str.replace('-', '/')
recovery_df['Week Begin date'] = pd.to_datetime(recovery_df['Week Begin date'], errors='coerce',format='%d/%m/%Y')
start_date='2022-06-20'
rec =recovery_df[(recovery_df['Week Begin date'] >= (start_date))]
rec['Week Begin date']=rec['Week Begin date'].astype(str)
recoverys=rec.iloc[:,[1,2,3,4,5,6,7,8,9]]
collection_df = recoverys.append(lsp_df,ignore_index = True)
collection_df.dropna(inplace = True)
sheet= clients.open_by_key(newleasingmaster)
ws= sheet.worksheet_by_title('Collection')
ws.clear(start='A586',end='I100000')
ws.set_dataframe(collection_df,(586,1))
print("New leasing master-collection tab updated successfully")