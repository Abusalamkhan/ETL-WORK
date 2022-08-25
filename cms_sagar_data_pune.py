#importing libraries

from hashlib import new
from tokenize import blank_re
import pygsheets #Importing python in google sheets
import pandas as pd #importing pandas
from pandas import Series,DataFrame
import datetime
from datetime import datetime, timedelta,date
import numpy as np
import datetime as dt



gc = pygsheets.authorize(service_file='client_secret.json')


year = int(input('Enter a year:'))
month = int(input('Enter a month:'))
day = int(input('Enter a day:'))



last_week_day = datetime(year, month, day)

print(last_week_day)

today = date.today()




next_day = last_week_day + timedelta(days = 1)

print(next_day)


#recovery tab

car_master_sheet_pune = gc.open("Pune Car Master Sheet")
cms_pune_recovery_tab = car_master_sheet_pune.worksheet_by_title("Recovery")
cms_pune_recovery_data = pd.DataFrame(cms_pune_recovery_tab.get_all_records())
cms_pune_recovery_data = cms_pune_recovery_data[['Date','ETM','Amount','DM Name']]
cms_pune_recovery_data.rename(columns={'DM Name':"Remarks"},inplace = True)
cms_pune_recovery_data['Type'] = 'RECOVERY'
print(cms_pune_recovery_data.head())
cms_pune_recovery_data = cms_pune_recovery_data[['Date','ETM','Amount','Remarks','Type']]
print(cms_pune_recovery_data.head())


#penalty tab

cms_pune_penalty_tab = car_master_sheet_pune.worksheet_by_title("Penalty")
cms_pune_penalty_data = pd.DataFrame(cms_pune_penalty_tab.get_all_records())
cms_pune_penalty_data = cms_pune_penalty_data[['Date in Payout','ETM','Penalty','Fine Amount']]
cms_pune_penalty_data.rename(columns={"Date in Payout":"Date",'Penalty':"Remarks","Fine Amount":"Amount"},inplace = True)
cms_pune_penalty_data['Type'] = np.where((cms_pune_penalty_data['Remarks'] == "Without Intimation"), "UNSCHEDULED_LEAVES", "ACCIDENT")
print(cms_pune_penalty_data.head())

#b2b tab

# cms_mumbai_B2B_tab = car_master_sheet_mumbai.worksheet_by_title("B2B")
# cms_mumbai_B2B_data = pd.DataFrame(cms_mumbai_B2B_tab.get_all_records())
# cms_mumbai_B2B_data = cms_mumbai_B2B_data[['Date','ETMID','Duty','TotalDutyPayout','Fuel','Toll']]
# cms_mumbai_B2B_data.rename(columns={"ETMID":"ETM",'Duty':"Remarks"},inplace = True)

# cms_mumbai_B2B_duty = cms_mumbai_B2B_data[['Date','ETM','Remarks','TotalDutyPayout']]
# cms_mumbai_B2B_duty['Type'] = 'B2B_DUTY'
# cms_mumbai_B2B_duty.rename(columns={"TotalDutyPayout":"Amount"},inplace = True)

# cms_mumbai_B2B_fuel = cms_mumbai_B2B_data[['Date','ETM','Remarks','Fuel']]
# cms_mumbai_B2B_fuel['Type'] = 'B2B_FUEL'
# cms_mumbai_B2B_fuel.rename(columns={"Fuel":"Amount"},inplace = True)


# cms_mumbai_B2B_toll = cms_mumbai_B2B_data[['Date','ETM','Remarks','Toll']]
# cms_mumbai_B2B_toll['Type'] = 'B2B_TOLL'
# cms_mumbai_B2B_toll.rename(columns={"Toll":"Amount"},inplace = True)

#rto tab

cms_pune_rto_tab = car_master_sheet_pune.worksheet_by_title("RTO")
cms_pune_rto_data = pd.DataFrame(cms_pune_rto_tab.get_all_records())
cms_pune_rto_data = cms_pune_rto_data[['Date in Payout','ETM','Penalty','Fine Amount']]
cms_pune_rto_data.rename(columns={"Date in Payout":"Date",'Penalty':"Remarks","Fine Amount":"Amount"},inplace = True)
cms_pune_rto_data['Type'] = 'RTO_FINE'
print(cms_pune_rto_data.head())

#adjustment

cms_pune_adjustment_tab = car_master_sheet_pune.worksheet_by_title("Adjustments")
cms_pune_adjustment_data = pd.DataFrame(cms_pune_adjustment_tab.get_all_records())
cms_pune_adjustment_data = cms_pune_adjustment_data[['Date','ETM ID','Type','Remark','Amount']]
cms_pune_adjustment_data.rename(columns={"ETM ID":"ETM",'Remark':"Remarks","Type":"Test Type"},inplace = True)

cms_pune_adjustment_data['Amount'] = cms_pune_adjustment_data['Amount'].replace(',','', regex=True)

cms_pune_adjustment_data[['Amount']] = cms_pune_adjustment_data[['Amount']].apply(pd.to_numeric)

col         = 'Test Type'
col1 = 'Amount'
conditions  = [ cms_pune_adjustment_data[col].str.contains("fuel",case = False),cms_pune_adjustment_data[col].str.contains("refer",case = False), cms_pune_adjustment_data[col].str.contains("life",case = False), cms_pune_adjustment_data[col].str.contains("repair",case = False),cms_pune_adjustment_data[col].str.contains("pun",case = False),cms_pune_adjustment_data[col].str.contains("toll",case = False),cms_pune_adjustment_data[col].str.contains("Penalty",case = False),cms_pune_adjustment_data[col].str.contains("reversal",case = False),cms_pune_adjustment_data[col].str.contains("joining fee",case = False),cms_pune_adjustment_data[col1] >= 0,cms_pune_adjustment_data[col1] < 0 ]
choices     = [ "FUEL_ADJUSTMENT","DRIVER_REFERENCE", 'LIFETIME_INCENTIVE', 'REPAIRS','REPAIRS','TOLL','PENALTY_REVERSAL','PENALTY_REVERSAL','JOINING_FEE','OTHER_ADDITIONS','OTHER_DEDUCTIONS']
    
cms_pune_adjustment_data["Type"] = np.select(conditions, choices, default=np.nan)

cms_pune_adjustment_data = cms_pune_adjustment_data[['Date','ETM','Remarks','Amount','Type']]
print(cms_pune_adjustment_data.head())

#amount paid

cms_pune_amount_paid_tab = car_master_sheet_pune.worksheet_by_title("Amount Paid")
cms_pune_amount_paid_data = pd.DataFrame(cms_pune_amount_paid_tab.get_all_records())
cms_pune_amount_paid_data = cms_pune_amount_paid_data[['Date','ETM','Remarks','Amount']]
cms_pune_amount_paid_data['Type'] = 'BANK_TRANSFER'
print(cms_pune_amount_paid_data.head())


#concating 

final_pune_data = pd.concat([cms_pune_recovery_data, cms_pune_amount_paid_data,cms_pune_adjustment_data,cms_pune_rto_data,cms_pune_penalty_data],ignore_index = True)
final_pune_data['Date'] = pd.to_datetime(final_pune_data['Date'],dayfirst=True)
final_pune_data['start date'] = last_week_day
final_pune_data['end date'] = next_day
final_pune_data = final_pune_data.loc[(final_pune_data['start date'] <= final_pune_data['Date']) & (final_pune_data['end date'] > final_pune_data['Date'])]
final_pune_data['ETM_NEW'] = final_pune_data['ETM'].str.upper() 
print(final_pune_data.head())


final_pune_data = final_pune_data[['ETM_NEW','Amount','Type','Remarks','Date']]
final_pune_data.rename(columns={"ETM_NEW":"ETM"},inplace = True)
final_pune_data_first =  final_pune_data.groupby(['ETM','Type'],as_index = False).first()
final_pune_data_first.drop(['Amount'],axis = 1, inplace = True)
final_pune_data = final_pune_data[['ETM','Amount','Type']]
final_pune_data['Amount'] = final_pune_data['Amount'].replace(',','', regex=True)
final_pune_data[['Amount']] = final_pune_data[['Amount']].apply(pd.to_numeric)
print(final_pune_data.head())

final_pune_data_sum =  final_pune_data.groupby(['ETM','Type'],as_index = False).sum()
final_pune_data= final_pune_data_sum.merge(final_pune_data_first, on=["ETM","Type"])
final_pune_data = final_pune_data.loc[final_pune_data['Amount'] != 0]
final_pune_data = final_pune_data[['Date','ETM','Type','Amount','Remarks']]
print(final_pune_data.head())

#pushing the final result

cms_test = gc.open("testing")
final_result = cms_test.worksheet_by_title("cms_pune")
final_result.set_dataframe(final_pune_data,'A1')