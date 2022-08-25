import MySQLdb
from MySQLdb import Error
import pandas as pd
import pygsheets #Importing python in google sheets

localhost = "localhost" 
pythonuser = "root" #user_name of database
pythonpwd123 = "Cavis_1234" #password 
database = "jarvis" #database to connect to

# localhost = "everest.casyoiozkia2.ap-south-1.rds.amazonaws.com" 
# pythonuser = "himani" #user_name of database
# pythonpwd123 = "Himani@EverestFleet@1234" #password 
# database = "jarvis" #database to connect to

try:
    dbconnect = MySQLdb.connect(host = localhost, user = pythonuser, password = pythonpwd123, database = database)
    
    if dbconnect.get_server_info():
        print("Connected to MySQL Server, version is",dbconnect.get_server_info())
        cur = dbconnect.cursor()
        
        cur.execute('SELECT car_number, Chassis_Number FROM  fleet_car WHERE city_id = 1')
        Jarvis_record = cur.fetchall()
        Jarvis_chassis_df = pd.DataFrame(Jarvis_record)
        Jarvis_chassis=Jarvis_chassis_df.set_axis(['car_number','Chassis_Number'], axis='columns')
        
        print(Jarvis_chassis.head(10))
        
        #Creating Connection for google sheet and keys
        
        client = pygsheets.authorize(service_file='client_secret.json')
        commitment_mapping_3='1sy3Gxrnh8bX6ibpGR6X8rba8H2IfeSNdBGT7CIFi3SY'
        car_status_report='1CfqvArNmTofvNOFAhQ965B7cMA7lX40e3RZZKY6IUjI'
        terrific='1f-DJ5O3zKKkAtXfXQghWzbsoXyy-ipdJ5q7Yma4tF9M'
        roaring='1MiZZY9MPEhhg-B9LyfmThQGV3Dy3XnKpb5NqgaYFwIQ'
        silent='1ZwXyZPyt7qhjTO5VkfwoGTJ48fmg_38MTdy4EHdVhwA'
        deep='1TPRKPsQfy4qY19byAuYi0Ie3Xld7ddrrBS8xqhggtcM'
        black='1KD0ABDWp3YvqIkmoQAZKRS-7-S8i-HtlO4KKpBMB1tU'
        
        #calculation and merging car status report and jarvis
        sheet= client.open_by_key(car_status_report)
        ws= sheet.worksheet_by_title('Cars')
        data = ws.get_all_values()
        headers = data.pop(0)
        cars_tab = pd.DataFrame(data,columns=headers)
        all_team=cars_tab[cars_tab['Current DM'].isin(['Deep Hunters', 'Silent Killers', 'Terrific Tigers', 'Black Panthers', 'Roaring Lions'])]
        all_team.rename({'Car Number':'car_number'},axis=1,inplace=True)
        all_team=all_team.loc[:,['car_number','Current DM']]
        car_status_df_and_chassis=all_team.merge(Jarvis_chassis, on='car_number',how='left')
        print(car_status_df_and_chassis.head(10))
        
        
        #To all teams
        deep_car=car_status_df_and_chassis[car_status_df_and_chassis['Current DM'].isin(['Deep Hunters'])]
        silent_car=car_status_df_and_chassis[car_status_df_and_chassis['Current DM'].isin(['Silent Killers'])]
        terrific_car=car_status_df_and_chassis[car_status_df_and_chassis['Current DM'].isin(['Terrific Tigers'])]
        black_car=car_status_df_and_chassis[car_status_df_and_chassis['Current DM'].isin(['Black Panthers'])]
        roaring_car=car_status_df_and_chassis[car_status_df_and_chassis['Current DM'].isin(['Roaring Lions'])]

        
        #pushing to sheets
        
        sheet= client.open_by_key(commitment_mapping_3)
        ws= sheet.worksheet_by_title('Vehicle_chassis_no')
        ws.clear(start='A',end='C')
        ws.set_dataframe(car_status_df_and_chassis,(1,1))
        
        sheet= client.open_by_key(terrific)
        ws= sheet.worksheet_by_title('Vehicle_chassis_no')
        ws.clear(start='A',end='C')
        ws.set_dataframe(terrific_car,(1,1))
        
        sheet= client.open_by_key(roaring)
        ws= sheet.worksheet_by_title('Vehicle_chassis_no')
        ws.clear(start='A',end='C')
        ws.set_dataframe(roaring_car,(1,1))
        
        sheet= client.open_by_key(silent)
        ws= sheet.worksheet_by_title('Vehicle_chassis_no')
        ws.clear(start='A',end='C')
        ws.set_dataframe(silent_car,(1,1))
        
        sheet= client.open_by_key(deep)
        ws= sheet.worksheet_by_title('Vehicle_chassis_no')
        ws.clear(start='A',end='C')
        ws.set_dataframe(deep_car,(1,1))
        
        sheet= client.open_by_key(black)
        ws= sheet.worksheet_by_title('Vehicle_chassis_no')
        ws.clear(start='A',end='C')
        ws.set_dataframe(black_car,(1,1))
            
except Error as e:
    print("Error:",e)