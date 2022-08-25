#importing libraries

import MySQLdb
from MySQLdb import Error
import pandas as pd
import pygsheets 
import pandas as pd
import datetime
from datetime import date, timedelta
import pygsheets
import warnings
warnings.filterwarnings("ignore")

localhost = "localhost" 
pythonuser = "root" #user_name of database
pythonpwd123 = "Cavis_1234" #password 
database = "jarvis" #database to connect to

# clients = pygsheets.authorize(service_file='car-master-sheet.json')
clients = pygsheets.authorize(service_file='/home/ubuntu/scripts/car-master-sheet.json')


#sheet keys

car_status_report='1CfqvArNmTofvNOFAhQ965B7cMA7lX40e3RZZKY6IUjI'
fleet_driver='1qceRS8LU17n5YWvgewcpJZif02KxV8Mtvs6anomHD80'
car_master='1_r5OMN1P8Tof5IRaE5jYd-jHaP8j15avQVMycDJdhec'
uber_ws='C:\\Users\\sagar\\Dropbox\\Dropbox\\DM Dashboard\\Master View.xlsx' 
dps_ws='C:\\Users\\sagar\\Dropbox\\Dropbox\\DM Dashboard\\Driver Performance Sheet.xlsx'
car_servicing_schedule_calling_servicing_tab='11WVBiisNIF8Xb7sx7GZEok7mG4Lz3uo96-e00xVtW0M'
commitment_mapping_3='1sy3Gxrnh8bX6ibpGR6X8rba8H2IfeSNdBGT7CIFi3SY'

#5 sheets keys

terrific='1f-DJ5O3zKKkAtXfXQghWzbsoXyy-ipdJ5q7Yma4tF9M'
roaring='1MiZZY9MPEhhg-B9LyfmThQGV3Dy3XnKpb5NqgaYFwIQ'
silent='1ZwXyZPyt7qhjTO5VkfwoGTJ48fmg_38MTdy4EHdVhwA'
deep='1TPRKPsQfy4qY19byAuYi0Ie3Xld7ddrrBS8xqhggtcM'
black='1KD0ABDWp3YvqIkmoQAZKRS-7-S8i-HtlO4KKpBMB1tU'
car_no_master_list='11D8_6u4ywy3yNYyrnMonyti6eflqUtpMfsxgLMBXEzk'

try:
    dbconnect = MySQLdb.connect(host = localhost, user = pythonuser, password = pythonpwd123, database = database)
        
    if dbconnect.get_server_info():
        print("Connected to MySQL Server, version is",dbconnect.get_server_info())
        cur = dbconnect.cursor()
        
        #uber
        cur.execute("SELECT date,fare_total,fare_per_hour_online,fare_per_km,trips,hours_online,trips_per_hour,km_per_trip,total_km,acceptance_rate_perc,driver_cancellation_rate,car_id FROM fleet_dailytrip WHERE date between date_sub(now(),INTERVAL 6 WEEK) and now()")
        Fleet_daily_trips_record = cur.fetchall()
        Fleet_daily_trips_record_df = pd.DataFrame(Fleet_daily_trips_record)
        Fleet_daily_trips_record_all_data=Fleet_daily_trips_record_df.set_axis(['date','fare_total','fare_per_hour_online','fare_per_km','trips','hours_online','trips_per_hour','km_per_trip','total_km','acceptance_rate_perc','driver_cancellation_rate','car_id'], axis='columns')        
        Fleet_daily_trips_record_all_data['date'] = pd.to_datetime(Fleet_daily_trips_record_all_data['date'], errors='coerce',format='%Y-%m-%d')
        #fleet_car for comparing car_id and car_number

        cur.execute("SELECT id,car_number,Chassis_Number FROM fleet_car")
        fleet_car_record=cur.fetchall()
        fleet_car_record_data = pd.DataFrame(fleet_car_record)
        fleet_car_record_data_df=fleet_car_record_data.set_axis(['id','car_number','Chassis_Number'],axis='columns')
        fleet_car_record_data_df.rename({'id':'car_id'},axis=1,inplace=True)
        
        #left merging for car number and vehicle chassis number
        
        Fleet_daily_trips_record_data_car_no=Fleet_daily_trips_record_all_data.merge(fleet_car_record_data_df, on='car_id',how='left')
        
        #fleet_car_team for mapping city_id
        
        cur.execute("SELECT car_id, city_id FROM fleet_car_team")
        fleet_car_team_record=cur.fetchall()
        fleet_car_team_record_data = pd.DataFrame(fleet_car_team_record)
        fleet_car_team_record_data_df=fleet_car_team_record_data.set_axis(['car_id','city_id'],axis='columns')
        print(fleet_car_team_record_data_df)
        
        #left merging for city_id
        
        Fleet_daily_trips_record_all_data=Fleet_daily_trips_record_data_car_no.merge(fleet_car_team_record_data_df,on='car_id',how='left')
        
        Fleet_daily_trips_record_data=Fleet_daily_trips_record_all_data[Fleet_daily_trips_record_all_data['city_id']==1]

                        
        print(Fleet_daily_trips_record_data.columns)
        print(Fleet_daily_trips_record_data.head())
        
        test_sheet = clients.open("testing")
        final_result = test_sheet.worksheet_by_title("daily_trips")
        final_result.set_dataframe(Fleet_daily_trips_record_data,'A1') 
        
        #trips of previous 7 days 
        
        today = datetime.date.today()
        print(today)
        this_week_monday = today - datetime.timedelta(days=today.weekday())
        print(this_week_monday)
        previous_monday_week= this_week_monday - datetime.timedelta(days=35)
        print(previous_monday_week)
        today_1 = today- timedelta(days=1)
        print(today_1)
        today_2 = today- timedelta(days=2)
        print(today_2)
        today_3 = today- timedelta(days=3)
        print(today_3)
        today_4 = today- timedelta(days=4)
        print(today_4)
        today_5 = today- timedelta(days=5)
        print(today_5) 
        today_6 = today- timedelta(days=6)
        print(today_6)
        today_7 = today- timedelta(days=7)
        print(today_7)   
        trips_fare_hours_previous_columns = Fleet_daily_trips_record_data[(Fleet_daily_trips_record_data['date'] >= pd.to_datetime(previous_monday_week)) & (Fleet_daily_trips_record_data['date'] <= pd.to_datetime(today))]
        
        trips_fare_hours_previous_columns_df_1=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_1))]
        trips_fare_hours_previous_columns_df_1=trips_fare_hours_previous_columns_df_1[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_1.rename({'trips':((today_1.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_1.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_1.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)
        print(trips_fare_hours_previous_columns_df_1)
        
        trips_fare_hours_previous_columns_df_2=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_2))]
        trips_fare_hours_previous_columns_df_2=trips_fare_hours_previous_columns_df_2[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_2.rename({'trips':((today_2.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_2.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_2.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)  
        
        trips_fare_hours_previous_columns_df_3=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_3))]
        trips_fare_hours_previous_columns_df_3=trips_fare_hours_previous_columns_df_3[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_3.rename({'trips':((today_3.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_3.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_3.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)
        
        trips_fare_hours_previous_columns_df_4=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_4))]
        trips_fare_hours_previous_columns_df_4=trips_fare_hours_previous_columns_df_4[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_4.rename({'trips':((today_4.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_4.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_4.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)       
        
        trips_fare_hours_previous_columns_df_5=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_5))]
        trips_fare_hours_previous_columns_df_5=trips_fare_hours_previous_columns_df_5[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_5.rename({'trips':((today_5.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_5.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_5.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)
        
        trips_fare_hours_previous_columns_df_6=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_6))]
        trips_fare_hours_previous_columns_df_6=trips_fare_hours_previous_columns_df_6[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_6.rename({'trips':((today_6.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_6.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_6.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)  
        
        trips_fare_hours_previous_columns_df_7=trips_fare_hours_previous_columns[(trips_fare_hours_previous_columns['date']==pd.to_datetime(today_7))]
        trips_fare_hours_previous_columns_df_7=trips_fare_hours_previous_columns_df_7[['date','car_number','trips','fare_total','hours_online']]
        trips_fare_hours_previous_columns_df_7.rename({'trips':((today_7.strftime("%Y-%m-%d"))+'.trip'),'fare_total':((today_7.strftime("%Y-%m-%d"))+'.fare_total'),'hours_online':((today_7.strftime("%Y-%m-%d"))+'.hours_online')},axis=1,inplace=True)  
        
             
        #comparing car status 
        
        cur.execute("SELECT date,status,car_id FROM fleet_carstatus")
        car_status_record=cur.fetchall()
        car_status_record_data = pd.DataFrame(car_status_record)
        car_status_record_data_df=car_status_record_data.set_axis(['date','status','car_id'],axis='columns')
        print(car_status_record_data_df)
        
        #left merging for car number
        fleet_car_record_data_df=fleet_car_record_data_df[['id','car_number']]
        car_status_record_data_car_no=car_status_record_data_df.merge(fleet_car_record_data_df, on='car_id',how='left')
        car_status_record_data_car_no['date']=pd.to_datetime(car_status_record_data_car_no['date'], errors='coerce',format='%Y-%m-%d')
        print(car_status_record_data_car_no)
        
        
        #merging everyday for status of the car
        
        status_1=trips_fare_hours_previous_columns_df_1.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_1=status_1[['car_number','status']]
        status_1.rename({'status':((today_1.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True)
        
        status_2=trips_fare_hours_previous_columns_df_2.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_2=status_2[['car_number','status']]
        status_2.rename({'status':((today_2.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True) 
        
        status_3=trips_fare_hours_previous_columns_df_3.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_3=status_3[['car_number','status']]
        status_3.rename({'status':((today_3.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True)
        
        status_4=trips_fare_hours_previous_columns_df_4.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_4=status_4[['car_number','status']]
        status_4.rename({'status':((today_4.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True)
                
        status_5=trips_fare_hours_previous_columns_df_5.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_5=status_5[['car_number','status']]
        status_5.rename({'status':((today_5.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True)
                
        status_6=trips_fare_hours_previous_columns_df_6.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_6=status_6[['car_number','status']]
        status_6.rename({'status':((today_6.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True)
        
        status_7=trips_fare_hours_previous_columns_df_7.merge(car_status_record_data_car_no,on=['date','car_number'],how='left')
        status_7=status_7[['car_number','status']]
        status_7.rename({'status':((today_7.strftime("%Y-%m-%d"))+'.status')},axis=1,inplace=True)
        
        all_car_status=status_1.merge(status_2,on='car_number').merge(status_3,on='car_number').merge(status_4,on='car_number').merge(status_5,on='car_number').merge(status_6,on='car_number').merge(status_7,on='car_number')
        print(all_car_status)
        
        
                               
        #trips+fare_total+hours_online
        
        trips_fare_hours_data_weekly = Fleet_daily_trips_record_data[(Fleet_daily_trips_record_data['date'] >= pd.to_datetime(this_week_monday)) & (Fleet_daily_trips_record_data['date'] <= pd.to_datetime(today))]        
        trips_fare_hours_data_weekly['fare_total']= pd.to_numeric(trips_fare_hours_data_weekly['fare_total'], errors = 'coerce')
        trips_fare_hours_data_weekly['trips']= pd.to_numeric(trips_fare_hours_data_weekly['trips'], errors = 'coerce')
        trips_fare_hours_data_weekly['hours_online']= pd.to_numeric(trips_fare_hours_data_weekly['hours_online'], errors = 'coerce')        
        trips_fare_hours_data = trips_fare_hours_data_weekly.groupby(['car_number',pd.Grouper(freq='W-MON', key='date')])['trips','fare_total','hours_online'].sum()
        trips_fare_hours_data_df=pd.DataFrame(trips_fare_hours_data).reset_index()
        trips_fare_hours_data_df['date']=trips_fare_hours_data_df['date'].astype(str)
        trips_fare_hours_data_df.rename({'trips':'Total trips'},axis=1,inplace=True)
        print(trips_fare_hours_data_df.dtypes)
        print(trips_fare_hours_data_df)
        
        test_sheet = clients.open("testing")
        final_result = test_sheet.worksheet_by_title("trips_fare_hours")
        final_result.set_dataframe(trips_fare_hours_data_df,'A1')
                 
        #Target

        cur.execute("SELECT week_start_date,week_end_date,slab_name,lower_limit_trips,upper_limit_trips FROM `fleet_car_incentive` WHERE slab_name='Mum Level 1'")
        Fleet_everest_rating_record = cur.fetchall()
        Fleet_everest_rating_record_data = pd.DataFrame(Fleet_everest_rating_record)        Fleet_everest_rating_record_df=Fleet_everest_rating_record_data.set_axis(['week_start_date','week_end_date','slab_name','lower_limit_trips','upper_limit_trips'],axis='columns')
        
        Fleet_everest_rating_record_df['week_start_date'] = pd.to_datetime(Fleet_everest_rating_record_df['week_start_date'], errors='coerce',format='%Y-%m-%d')
        Fleet_everest_rating_record_df['week_end_date'] = pd.to_datetime(Fleet_everest_rating_record_df['week_end_date'], errors='coerce',format='%Y-%m-%d')
        Fleet_everest_rating_record_today = Fleet_everest_rating_record_df[(Fleet_everest_rating_record_df['week_start_date'] >= pd.to_datetime(previous_monday_week)) & (Fleet_everest_rating_record_df['week_end_date']>=pd.to_datetime(previous_monday_week))]
        print(Fleet_everest_rating_record_today)        
        
        trips_fare_hours_data_df['Target']=Fleet_everest_rating_record_today['lower_limit_trips']
        print(trips_fare_hours_data_df['Target'])
        
        #balance trips (target-total trips)
        
        trips_fare_hours_data_df['Balance trips']=trips_fare_hours_data_df['Target']-trips_fare_hours_data_df['Total trips']
        print(trips_fare_hours_data_df['Balance trips'])
                
        #last week trips

        cur.execute("SELECT start_date,end_date,car_number,trips FROM `fleet_vehicledata`")
        last_week_trips_record = cur.fetchall()
        last_week_trips_record_data = pd.DataFrame(last_week_trips_record)
        last_week_trips_record_data_df=last_week_trips_record_data.set_axis(['start_date','end_date','car_number','trips'],axis='columns')        
        last_week_trips_record_data_df_previous_monday_week=last_week_trips_record_data_df[(last_week_trips_record_data_df['start_date']==previous_monday_week)]       
        print(last_week_trips_record_data_df_previous_monday_week)
        
        ########################################   dps   ###########################################################################
        
        #fleet_gpsdata
        
        cur.execute("SELECT date,disance_km,car_id FROM fleet_gpsdata WHERE date between date_sub(now(),INTERVAL 6 WEEK) and now()")
        fleet_gpsdata_record=cur.fetchall()
        fleet_gpsdata_record_data = pd.DataFrame(fleet_gpsdata_record)
        fleet_gpsdata_record_data_df=fleet_gpsdata_record_data.set_axis(['date','disance_km','car_id'],axis='columns')
        fleet_gpsdata_record_data_df.rename({'disance_km':'kms'},axis=1,inplace=True)
        
        # kms- left merging for car_number
        
        fleet_car_record_data_kms=fleet_gpsdata_record_data_df.merge(fleet_car_record_data_df, on='car_id',how='left')
        print(fleet_car_record_data_kms)
                
        #dead_kms
        
        Fleet_daily_trips_record_data_copy=Fleet_daily_trips_record_data.copy()
        print(Fleet_daily_trips_record_data_copy.dtypes)
        Fleet_daily_trips_record_data_copy_df=Fleet_daily_trips_record_data_copy.merge(fleet_car_record_data_kms,on=['date','car_number'],how='left')
        print(Fleet_daily_trips_record_data_copy_df)
        Fleet_daily_trips_record_data_copy_df['dead km']=Fleet_daily_trips_record_data_copy_df['total_km']-Fleet_daily_trips_record_data_copy_df['kms']
        print(Fleet_daily_trips_record_data_copy_df['dead km'])
        Fleet_daily_trips_record_data_copy_df=Fleet_daily_trips_record_data_copy_df[['date','trips','total_km','car_number','kms','dead km']]
        #allowance
        
        Fleet_daily_trips_record_data_copy_df['trips']= pd.to_numeric(Fleet_daily_trips_record_data_copy_df['trips'], errors = 'coerce')
        Fleet_daily_trips_record_data_copy_df['allowance']=15+Fleet_daily_trips_record_data_copy_df['trips']*3
        Fleet_daily_trips_record_data_copy_df['allowance']=Fleet_daily_trips_record_data_copy_df['allowance'].replace('15','0')
        print(Fleet_daily_trips_record_data_copy_df['allowance'])
        
        #deviation
        
        Fleet_daily_trips_record_data_copy_df['deviation']=Fleet_daily_trips_record_data_copy_df['dead km']-Fleet_daily_trips_record_data_copy_df['allowance']        
        print(Fleet_daily_trips_record_data_copy_df['deviation'])
        
        test_sheet = clients.open("testing")
        final_result = test_sheet.worksheet_by_title("dead_kms")
        final_result.set_dataframe(Fleet_daily_trips_record_data_copy_df,'A1')
        
        
except Error as e:
    print("Error:",e)