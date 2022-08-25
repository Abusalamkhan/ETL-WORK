# importing libraries

import pandas as pd
from pandas import Series,DataFrame
import datetime
from datetime import date, timedelta
import pygsheets
import warnings
import numpy as np
warnings.filterwarnings("ignore")

clients = pygsheets.authorize(service_file='repair.json')

allotment_status_report='1cpR6AVVpk9TF4_I38IFYPPOqk-_bSROHgVVYdaXLXOI'
car_no_master_list='11D8_6u4ywy3yNYyrnMonyti6eflqUtpMfsxgLMBXEzk'
penalty_form_repair = '1Txa0MVP7Kxxjhwjd5EwME3JfEPSi5q2klC7GuZw2hRc'
testing='1ELkL-R5Hs2FhOA7huIirzfebLxX57cN2v3wFQnKrupY'
#car no master list

sheet= clients.open_by_key(car_no_master_list)
ws= sheet.worksheet_by_title('History')
data = ws.get_all_values()
headers = data.pop(0)
car_no_master_list = pd.DataFrame(data,columns=headers)
car_no_master_list=car_no_master_list[['Date', 'Car No','DM','End']]
car_no_master_list['Car No']=car_no_master_list['Car No'].str.replace(' ','')
car_no_master_list['Date']=pd.to_datetime(car_no_master_list['Date'], errors='coerce',format='%d-%b-%Y')
car_no_master_list['End']=pd.to_datetime(car_no_master_list['End'], errors='coerce',format='%d/%m/%Y')
car_no_master_list.rename({'Car No':'Car Number'},axis=1,inplace=True)


#allotment status report sheet

sheet = clients.open_by_key(allotment_status_report)
ws = sheet.worksheet_by_title('New Allotment History')
data = ws.get_all_values()
headers = data.pop(0)
allotment_s_r = pd.DataFrame(data,columns=headers)
allotment_s_r = allotment_s_r[['Allotment Date','Return Date','Car Number','ETM','DM Name']]
allotment_s_r['Car Number']=allotment_s_r['Car Number'].str.replace(' ','')
today = datetime.datetime.now()
today_timestmp_column = today.strftime("%d/%m/%Y %H:%M:%S")
allotment_s_r['Allotment Date']=allotment_s_r['Allotment Date'].replace('',(today_timestmp_column))
allotment_s_r['Return Date']=allotment_s_r['Return Date'].replace('',(today_timestmp_column))
allotment_s_r[['Allotment Date','Return Date']]=allotment_s_r[['Allotment Date','Return Date']].apply(pd.to_datetime, errors='coerce',format='%d/%m/%Y %H:%M:%S')


#penalty form sheet

sheet= clients.open_by_key(penalty_form_repair)
ws= sheet.worksheet_by_title('Form responses')
data = ws.get_all_values()
headers = data.pop(0)
responses_df = pd.DataFrame(data,columns=headers)
responses_df=responses_df[['Timestamp', 'Car Number', 'Panel Types']]
responses_df['Car Number']=responses_df['Car Number'].str.replace(' ','')
responses_df['Timestamp']=pd.to_datetime(responses_df['Timestamp'], errors='coerce',format='%d/%m/%Y %H:%M:%S')

#merging allotment 

allotment_car_no=responses_df.merge(allotment_s_r, on='Car Number',how='left')
allotment_car_no['Timestamp']=pd.to_datetime(allotment_car_no['Timestamp']).dt.strftime('%Y-%m-%d')
allotment_car_no_df = allotment_car_no[(allotment_car_no['Allotment Date'] <= allotment_car_no['Timestamp']) & (allotment_car_no['Timestamp'] <= allotment_car_no['Return Date'])]
allotment_car_no_df=allotment_car_no_df[['Car Number','ETM']]
allotment_car_no_df
    
#merging car_no_master_list

car_no_master_dm_name=responses_df.merge(car_no_master_list, on='Car Number',how='left')
car_no_master_dm_name_df = car_no_master_dm_name[(car_no_master_dm_name['Date'] <= car_no_master_dm_name['Timestamp']) & (car_no_master_dm_name['Timestamp'] <= car_no_master_dm_name['End'])]
car_no_master_dm_name_df=car_no_master_dm_name_df[['Car Number','DM']]

#merging allotment_and_car_no

allotment_etm_and_car_no_master=allotment_car_no_df.merge(car_no_master_dm_name_df, on='Car Number', how='left')

#penalty form sheet


responses_df_new = responses_df['Panel Types'].str.split(',').apply(Series,1).stack()

print(responses_df_new.head(10))

responses_df_new.index = responses_df_new.index.droplevel(-1)

responses_df_new.name = 'Panel Types'

print(responses_df_new.head())

del responses_df['Panel Types']

amount_final = responses_df.join(responses_df_new)

print(amount_final)

amount_final_df=amount_final.merge(allotment_etm_and_car_no_master, on='Car Number',how='left')

amount_final_df[['Panel name','Amount']]= amount_final_df['Panel Types'].str.split('-',expand=True)



print(amount_final_df)

del amount_final_df['Panel Types']

amount_final_df['Timestamp']=amount_final_df['Timestamp'].astype(str)
amount_final_df['Timestamp'].replace('NaT','', inplace=True,regex=True)

amount_final_df.drop_duplicates(inplace=True)

# amount_final_df['ETM'].fillna('',inplace=True)
# amount_final_df['DM'].fillna('',inplace=True)
# amount_final_df['Amount'].fillna('',inplace=True)

# amount_final_df['ETM']=amount_final_df['ETM'].str.replace('Leasing','')
# amount_final_df['ETM']=amount_final_df['ETM'].str.replace('Insurance','')
# amount_final_df['ETM']=amount_final_df['ETM'].str.replace('Repairing','')
# amount_final_df['ETM']=amount_final_df['ETM'].str.replace('Vinay Bhai','')

# amount_final_df['DM']=amount_final_df['DM'].str.replace('Deadly Shark','')
# amount_final_df['DM']=amount_final_df['DM'].str.replace('Red Eagle','')
# amount_final_df['DM']=amount_final_df['DM'].str.replace('Vinay Bhai Leasing','')
# amount_final_df['DM']=amount_final_df['DM'].str.replace('Vinay Bhai','')


car_no_master_list_dm_tab= car_no_master_list_sheet.worksheet_by_title('DM')
data = car_no_master_list_dm_tab.get_all_values()
headers = data.pop(0)
car_no_master_list_dm_data = pd.DataFrame(data,columns=headers)
mumbai_60_40=car_no_master_list_dm_data[car_no_master_list_dm_data['Cities']=='Mumbai 60:40']

amount_final_df=amount_final_df[amount_final_df['DM'].isin(mumbai_60_40['DM NAME'])]
amount_final_df


print(amount_final_df.head())
print(amount_final_df.shape)

# amount_final_df['DM'] = np.where(
#     amount_final_df['ETM'] == '',
#     '',
#     amount_final_df['DM']
# )        

sheet= clients.open_by_key(penalty_form_repair)
ws= sheet.worksheet_by_title('Penalty_amount')
ws.clear()
ws.set_dataframe(amount_final_df,(1,1))