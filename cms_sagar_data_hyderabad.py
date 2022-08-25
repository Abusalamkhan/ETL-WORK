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


#recovery
car_master_sheet_hyderabad = gc.open("Hyderabad Car Master Sheet")
cms_hyderabad_recovery_tab = car_master_sheet_hyderabad.worksheet_by_title("Recovery")
cms_hyderabad_recovery_data = pd.DataFrame(cms_hyderabad_recovery_tab.get_all_records())
cms_hyderabad_recovery_data = cms_hyderabad_recovery_data[['Payment Date','Pilot ETH ID','Amount','DM Name']]
cms_hyderabad_recovery_data.rename(columns={"Payment Date":"Date","Pilot ETH ID":"ETH",'DM Name':"Remarks"},inplace = True)
cms_hyderabad_recovery_data['Type'] = 'RECOVERY'
cms_hyderabad_recovery_data = cms_hyderabad_recovery_data[['Date','ETH','Amount','Remarks','Type']]

#penalty
cms_hyderabad_penalty_tab = car_master_sheet_hyderabad.worksheet_by_title("Penalty")
cms_hyderabad_penalty_data = pd.DataFrame(cms_hyderabad_penalty_tab.get_all_records())
cms_hyderabad_penalty_data = cms_hyderabad_penalty_data[['Date in Payout','ETM','Penalty','Fine Amount']]
cms_hyderabad_penalty_data.rename(columns={"Date in Payout":"Date","ETM":"ETH",'Penalty':"Remarks","Fine Amount":"Amount"},inplace = True)
cms_hyderabad_penalty_data['Type'] = np.where((cms_hyderabad_penalty_data['Remarks'] == "Without Intimation"), "UNSCHEDULED_LEAVES", "ACCIDENT")

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
cms_hyderabad_rto_tab = car_master_sheet_hyderabad.worksheet_by_title("RTO")
cms_hyderabad_rto_data = pd.DataFrame(cms_hyderabad_rto_tab.get_all_records())
cms_hyderabad_rto_data = cms_hyderabad_rto_data[['Date in Payout','ETM No','Penalty','Fine Amount']]
cms_hyderabad_rto_data.rename(columns={"Date in Payout":"Date",'ETM No':'ETH','Penalty':"Remarks","Fine Amount":"Amount"},inplace = True)
cms_hyderabad_rto_data['Type'] = 'RTO_FINE'

#adjustment
cms_hyderabad_adjustment_tab = car_master_sheet_hyderabad.worksheet_by_title("Adjustments")
cms_hyderabad_adjustment_data = pd.DataFrame(cms_hyderabad_adjustment_tab.get_all_records())
cms_hyderabad_adjustment_data = cms_hyderabad_adjustment_data[['Date','ETM','type','Remark','Amount']]
cms_hyderabad_adjustment_data.rename(columns={"ETM":"ETH",'Remark':"Remarks","type":"Test Type"},inplace = True)

cms_hyderabad_adjustment_data['Amount'] = cms_hyderabad_adjustment_data['Amount'].replace(',','', regex=True)
cms_hyderabad_adjustment_data['Amount'] = cms_hyderabad_adjustment_data['Amount'].replace(' ','', regex=True)

cms_hyderabad_adjustment_data[['Amount']] = cms_hyderabad_adjustment_data[['Amount']].apply(pd.to_numeric)

col         = 'Test Type'
col1 = 'Amount'
conditions  = [ cms_hyderabad_adjustment_data[col].str.contains("fuel",case = False),cms_hyderabad_adjustment_data[col].str.contains("refer",case = False), cms_hyderabad_adjustment_data[col].str.contains("life",case = False), cms_hyderabad_adjustment_data[col].str.contains("repair",case = False),cms_hyderabad_adjustment_data[col].str.contains("pun",case = False),cms_hyderabad_adjustment_data[col].str.contains("toll",case = False),cms_hyderabad_adjustment_data[col].str.contains("Penalty",case = False),cms_hyderabad_adjustment_data[col].str.contains("reversal",case = False),cms_hyderabad_adjustment_data[col].str.contains("joining fee",case = False),cms_hyderabad_adjustment_data[col1] >= 0,cms_hyderabad_adjustment_data[col1] < 0 ]
choices     = [ "FUEL_ADJUSTMENT","DRIVER_REFERENCE", 'LIFETIME_INCENTIVE', 'REPAIRS','REPAIRS','TOLL','PENALTY_REVERSAL','PENALTY_REVERSAL','JOINING_FEE','OTHER_ADDITIONS','OTHER_DEDUCTIONS']
    
cms_hyderabad_adjustment_data["Type"] = np.select(conditions, choices, default=np.nan)

cms_hyderabad_adjustment_data = cms_hyderabad_adjustment_data[['Date','ETH','Remarks','Amount','Type']]

#amount paid
cms_hyderabad_amount_paid_tab = car_master_sheet_hyderabad.worksheet_by_title("Amount Paid")
cms_hyderabad_amount_paid_data = pd.DataFrame(cms_hyderabad_amount_paid_tab.get_all_records())
cms_hyderabad_amount_paid_data = cms_hyderabad_amount_paid_data[['Date','ETH','Remarks','Amount']]
cms_hyderabad_amount_paid_data['Type'] = 'BANK_TRANSFER'

#concating 

final_hyderabad_data = pd.concat([cms_hyderabad_recovery_data, cms_hyderabad_amount_paid_data,cms_hyderabad_adjustment_data,cms_hyderabad_rto_data,cms_hyderabad_penalty_data],ignore_index = True)
final_hyderabad_data['Date'] = pd.to_datetime(final_hyderabad_data['Date'],dayfirst=True)
final_hyderabad_data['start date'] = last_week_day
final_hyderabad_data['end date'] = next_day
final_hyderabad_data = final_hyderabad_data.loc[(final_hyderabad_data['start date'] <= final_hyderabad_data['Date']) & (final_hyderabad_data['end date'] > final_hyderabad_data['Date'])]
final_hyderabad_data['ETH_NEW'] = final_hyderabad_data['ETH'].str.upper() 

final_hyderabad_data = final_hyderabad_data[['ETH_NEW','Amount','Type','Remarks','Date']]
final_hyderabad_data.rename(columns={"ETH_NEW":"ETH"},inplace = True)

final_hyderabad_data_first =  final_hyderabad_data.groupby(['ETH','Type'],as_index = False).first()

final_hyderabad_data_first.drop(['Amount'],axis = 1, inplace = True)
final_hyderabad_data = final_hyderabad_data[['ETH','Amount','Type']]
final_hyderabad_data['Amount'] = final_hyderabad_data['Amount'].replace(',','', regex=True)
final_hyderabad_data[['Amount']] = final_hyderabad_data[['Amount']].apply(pd.to_numeric)

final_hyderabad_data_sum =  final_hyderabad_data.groupby(['ETH','Type'],as_index = False).sum()
final_hyderabad_data= final_hyderabad_data_sum.merge(final_hyderabad_data_first, on=["ETH","Type"])
final_hyderabad_data = final_hyderabad_data.loc[final_hyderabad_data['Amount'] != 0]
final_hyderabad_data = final_hyderabad_data[['Date','ETH','Type','Amount','Remarks']]
final_hyderabad_data

#pushing the final result

cms_test = gc.open("testing")
final_result = cms_test.worksheet_by_title("cms_hyd")
final_result.set_dataframe(final_hyderabad_data,'A1')
