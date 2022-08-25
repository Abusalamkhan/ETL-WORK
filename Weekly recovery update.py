#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import warnings
from twilio.rest import Client as c
warnings.filterwarnings("ignore")

#creating connections

scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)

try:
    aatmanirbhar_pune='1-wFeo4oUvXUasGZNw1LN3V-FSOFbOy2j0xIjLIIB1LE'
    
    sheet= client.open_by_key('1j9hmPHNmfjJp7nn4p8EVq8aciL8NfiO3hYWdf28aDzQ')#Recovery Sheet-New
    ws= sheet.worksheet('Masterview')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    df=dfm.iloc[:,[1,2,3,4,7,8,12,10]]
    df1w=df.loc[df['Collection of Purpose'].isin(['Mumbai 60:40 Recovery','Mumbai 60: 40 Joining Fees'])]
    sheet= client.open('Aatmanirbhar -3')
    ws= sheet.worksheet('WRU')
    #ws.batch_clear(['A:H'])
    ws.update([df1w.columns.values.tolist()] + df1w.values.tolist(),value_input_option='USER_ENTERED')
    print("Mumbai-Weekly recovery update updated succesfully")

    sheet= client.open_by_key('1j9hmPHNmfjJp7nn4p8EVq8aciL8NfiO3hYWdf28aDzQ')#Recovery Sheet-New
    ws= sheet.worksheet('Masterview')
    data = ws.get_all_values()
    headers = data.pop(0)
    dfm = pd.DataFrame(data,columns=headers)
    df=dfm.iloc[:,[1,2,3,4,7,8,12,10]]
    df1w=df.loc[df['Collection of Purpose'].isin(['Pune 60:40 Recovery','Pune 60: 40 Joining Fees'])]
    sheet= client.open_by_key(aatmanirbhar_pune)
    ws= sheet.worksheet('WRU')
    #ws.batch_clear(['A:H'])
    ws.update([df1w.columns.values.tolist()] + df1w.values.tolist(),value_input_option='USER_ENTERED')
    print("Pune-Weekly recovery update updated succesfully")

except Exception as e:
    number=['+91 81084 16708','+91 98200 66683']
    for to_number in number:
        account_sid="AC3459ee86068c97f9cd2de30ad98146e4"
        auth_token="208bac357247abfae8b25d89406dc608"
        client=c(account_sid,auth_token)
        from_number='+1 9705577507'
        error=str(e)
        error_message=error+" error in your Weekly recovery update code"        
        client.messages.create(
            body=error_message,
            from_=from_number,
            to=to_number)
        print(e,"error in your Weekly recovery update code")