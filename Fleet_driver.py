
#Fleet_driver

import MySQLdb
from MySQLdb import Error
import pandas as pd
import pygsheets #Importing python in google sheets

localhost = "everest.casyoiozkia2.ap-south-1.rds.amazonaws.com" 
pythonuser = "himani" #user_name of database
pythonpwd123 = "Himani@EverestFleet@1234" #password 
database = "jarvis" #database to connect to

try:
    dbconnect = MySQLdb.connect(host = localhost, user = pythonuser, password = pythonpwd123, database = database)
    
    if dbconnect.get_server_info():
        print("Connected to MySQL Server, version is",dbconnect.get_server_info())
        
        #getting from database
        
        cur = dbconnect.cursor()
        
        cur.execute('SELECT id, employee_id, name, mobile FROM fleet_driver WHERE city_id = 1')
        Jarvis_record = cur.fetchall()
        Jarvis_df = pd.DataFrame(Jarvis_record)
        Jarvis=Jarvis_df.set_axis(['id', 'employee_id', 'name','mobile'], axis='columns')
        
        cur.execute('SELECT id, city_id, employee_id, name, uber_name, mobile, alternate_number, location_id, source, reference, date_of_joining, date_of_exit, license_no, license_issue_date, license_expiry_date, status, aadhar_no, pan_no, permanent_address, uber_uuid, uber_device_no, type, is_active FROM fleet_driver')
        Fleet_driver_2_record = cur.fetchall()
        Fleet_driver_2_df = pd.DataFrame(Fleet_driver_2_record)
        Fleet_driver_2 =Fleet_driver_2_df.set_axis(['id', 'city_id', 'employee_id', 'name', 'uber_name', 'mobile', 'alternate_number', 'location_id', 'source', 'reference', 'date_of_joining', 'date_of_exit', 'license_no', 'license_issue_date', 'license_expiry_date', 'status', 'aadhar_no', 'pan_no', 'permanent_address', 'uber_uuid', 'uber_device_no', 'type', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, city_id, employee_id, name, uber_name, mobile, alternate_number, location_id, source, reference, date_of_joining, date_of_exit, uber_uuid, uber_device_no, type, is_active FROM fleet_driver WHERE city_id = 1 AND is_active = 1')
        etm_record = cur.fetchall()
        etm_df = pd.DataFrame(etm_record)
        etm=etm_df.set_axis(['id', 'city_id', 'employee_id', 'name', 'uber_name', 'mobile', 'alternate_number', 'location_id', 'source', 'reference', 'date_of_joining', 'date_of_exit', 'uber_uuid', 'uber_device_no', 'type', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, city_id, employee_id, name, uber_name, mobile, alternate_number, location_id, source, reference, date_of_joining, date_of_exit, uber_uuid, uber_device_no, type, is_active FROM fleet_driver WHERE city_id = 2 AND is_active = 1')
        etb_record = cur.fetchall()
        etb_df = pd.DataFrame(etb_record)
        etb=etb_df.set_axis(['id', 'city_id', 'employee_id', 'name', 'uber_name', 'mobile', 'alternate_number', 'location_id', 'source', 'reference', 'date_of_joining', 'date_of_exit', 'uber_uuid', 'uber_device_no', 'type', 'is_active'], axis='columns')

        cur.execute('SELECT id, city_id, employee_id, name, uber_name, mobile, alternate_number, location_id, source, reference, date_of_joining, date_of_exit, uber_uuid, uber_device_no, type, is_active FROM fleet_driver WHERE city_id = 3 AND is_active = 1')
        etn_record = cur.fetchall()
        etn_df = pd.DataFrame(etn_record)
        etn=etn_df.set_axis(['id', 'city_id', 'employee_id', 'name', 'uber_name', 'mobile', 'alternate_number', 'location_id', 'source', 'reference', 'date_of_joining', 'date_of_exit', 'uber_uuid', 'uber_device_no', 'type', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, city_id, employee_id, name, uber_name, mobile, alternate_number, location_id, source, reference, date_of_joining, date_of_exit, uber_uuid, uber_device_no, type, is_active FROM fleet_driver WHERE city_id = 4 AND is_active = 1')
        eth_record = cur.fetchall()
        eth_df = pd.DataFrame(eth_record)
        eth=eth_df.set_axis(['id', 'city_id', 'employee_id', 'name', 'uber_name', 'mobile', 'alternate_number', 'location_id', 'source', 'reference', 'date_of_joining', 'date_of_exit', 'uber_uuid', 'uber_device_no', 'type', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, city_id, employee_id, name, uber_name, mobile, alternate_number, location_id, source, reference, date_of_joining, date_of_exit, uber_uuid, uber_device_no, type, is_active FROM fleet_driver WHERE city_id = 5 AND is_active = 1')
        etp_record = cur.fetchall()
        etp_df = pd.DataFrame(etp_record)
        etp=etp_df.set_axis(['id', 'city_id', 'employee_id', 'name', 'uber_name', 'mobile', 'alternate_number', 'location_id', 'source', 'reference', 'date_of_joining', 'date_of_exit', 'uber_uuid', 'uber_device_no', 'type', 'is_active'], axis='columns')        
        
        print("Converted db to Fleet_driver dataframe succesfully")
        
        #Creating Connection for google sheet
        
        client = pygsheets.authorize(service_file='client_secret.json')
        
        #pushing to sheets
        
        sheet= client.open_by_key('1xQlorzAmjDzKVLGalIzxWaC3LnwMPNPaZz6oLLWPQ6M')#Fleet_driver_2 jarvis is for testing but need to push in car master jarvis
        ws= sheet.worksheet_by_title('Jarvis')
        ws.clear(start='A',end='D')
        ws.set_dataframe(Jarvis,(1,1))
        
        sheet= client.open_by_key('1xQlorzAmjDzKVLGalIzxWaC3LnwMPNPaZz6oLLWPQ6M')#Fleet_driver_2
        ws= sheet.worksheet_by_title('Fleet_driver')
        ws.clear(start='A',end='W')
        ws.set_dataframe(Fleet_driver_2,(1,1))
        
        sheet= client.open('mumbai_driver')
        ws= sheet.worksheet_by_title('Fleet_driver')
        ws.clear(start='A',end='P')
        ws.set_dataframe(etm,(1,1))
        
        sheet= client.open('bangalore_driver')
        ws= sheet.worksheet_by_title('Fleet_driver')
        ws.clear(start='A',end='P')
        ws.set_dataframe(etb,(1,1))
        
        sheet= client.open('newdelhi_driver')
        ws= sheet.worksheet_by_title('Fleet_driver')
        ws.clear(start='A',end='P')
        ws.set_dataframe(etn,(1,1))
        
        sheet= client.open('hyderabad_driver')
        ws= sheet.worksheet_by_title('Fleet_driver')
        ws.clear(start='A',end='P')
        ws.set_dataframe(eth,(1,1))
        
        sheet= client.open('pune_driver')
        ws= sheet.worksheet_by_title('Fleet_driver')
        ws.clear(start='A',end='P')
        ws.set_dataframe(etp,(1,1))
        
        print("Fleet_driver Sheet Updated Succesfully")

except Error as e:
    print("Error:",e)

# fleet_location

try:
    dbconnect = MySQLdb.connect(host = localhost, user = pythonuser, password = pythonpwd123, database = database)
    
    if dbconnect.get_server_info():
        print("Connected to MySQL Server, version is",dbconnect.get_server_info())
        
        #getting from database
        
        cur = dbconnect.cursor()
        
        cur.execute('SELECT id, city_id, name FROM fleet_location')
        Fleet_driver_2_record = cur.fetchall()
        Fleet_driver_2_df = pd.DataFrame(Fleet_driver_2_record)
        Fleet_driver_2 =Fleet_driver_2_df.set_axis(['id', 'city_id','name'], axis='columns')
        
        cur.execute('SELECT id, city_id, name FROM fleet_location WHERE city_id = 1')
        etm_record = cur.fetchall()
        etm_df = pd.DataFrame(etm_record)
        etm = etm_df.set_axis(['id', 'city_id','name'], axis='columns')
        
        cur.execute('SELECT id, city_id, name FROM fleet_location WHERE city_id = 2')
        etb_record = cur.fetchall()
        etb_df = pd.DataFrame(etb_record)
        etb = etb_df.set_axis(['id', 'city_id','name'], axis='columns')        
        
        cur.execute('SELECT id, city_id, name FROM fleet_location WHERE city_id = 3')
        etn_record = cur.fetchall()
        etn_df = pd.DataFrame(etn_record)
        etn = etn_df.set_axis(['id', 'city_id','name'], axis='columns')        
        
        cur.execute('SELECT id, city_id, name FROM fleet_location WHERE city_id = 4')
        eth_record = cur.fetchall()
        eth_df = pd.DataFrame(eth_record)
        eth = eth_df.set_axis(['id', 'city_id','name'], axis='columns')        
        
        cur.execute('SELECT id, city_id, name FROM fleet_location WHERE city_id = 5')
        etp_record = cur.fetchall()
        etp_df = pd.DataFrame(etp_record)
        etp = etp_df.set_axis(['id', 'city_id','name'], axis='columns')        
        
        print("Converted db to Fleet_location dataframe succesfully")
        
        #Creating Connection for google sheet
        
        client = pygsheets.authorize(service_file='client_secret.json')
        
        #pushing to sheets
                
        sheet= client.open_by_key('1xQlorzAmjDzKVLGalIzxWaC3LnwMPNPaZz6oLLWPQ6M')#Fleet_driver_2
        ws= sheet.worksheet_by_title('Fleet_location')
        ws.clear()
        ws.set_dataframe(Fleet_driver_2,(1,1))
        
        sheet= client.open('mumbai_driver')
        ws= sheet.worksheet_by_title('Fleet_location')
        ws.clear()
        ws.set_dataframe(etm,(1,1))
        
        sheet= client.open('bangalore_driver')
        ws= sheet.worksheet_by_title('Fleet_location')
        ws.clear()
        ws.set_dataframe(etb,(1,1))
        
        sheet= client.open('newdelhi_driver')
        ws= sheet.worksheet_by_title('Fleet_location')
        ws.clear()
        ws.set_dataframe(etn,(1,1))
        
        sheet= client.open('hyderabad_driver')
        ws= sheet.worksheet_by_title('Fleet_location')
        ws.clear()
        ws.set_dataframe(eth,(1,1))
        
        sheet= client.open('pune_driver')
        ws= sheet.worksheet_by_title('Fleet_location')
        ws.clear()
        ws.set_dataframe(etp,(1,1))
        
        print("Fleet_location Sheet Updated Succesfully")

except Error as e:
    print("Error:",e)
        
# Fleet_hiring

try:
    dbconnect = MySQLdb.connect(host = localhost, user = pythonuser, password = pythonpwd123, database = database)
    
    if dbconnect.get_server_info():
        print("Connected to MySQL Server, version is",dbconnect.get_server_info())
        
        #getting from database
        
        cur = dbconnect.cursor()
        
        cur.execute('SELECT id, name, mobile, city_id, driver_id, location_id, marital_status, age, shift, source, reference, other_source_reference, uber_id_status, uber_id_detailed_status, trainer_name, training_date, status, hr_date, onboarding_date, test_date, last_driven_partner_name, last_driven_partner_name FROM fleet_hiring')
        Fleet_driver_2_record = cur.fetchall()
        Fleet_driver_2_df = pd.DataFrame(Fleet_driver_2_record)
        Fleet_driver_2 =Fleet_driver_2_df.set_axis(['id', 'name', 'mobile', 'city_id', 'driver_id', 'location_id', 'marital_status', 'age', 'shift', 'source', 'reference', 'other_source_reference', 'uber_id_status', 'uber_id_detailed_status', 'trainer_name', 'training_date', 'status', 'hr_date', 'onboarding_date', 'test_date', 'last_driven_partner_name', 'last_driven_partner_name'], axis='columns')
        
        cur.execute('SELECT id, name, mobile, city_id, driver_id, location_id, marital_status, age, shift, source, reference, other_source_reference, uber_id_status, uber_id_detailed_status, trainer_name, training_date, status, hr_date, onboarding_date, test_date, last_driven_partner_name, last_driven_partner_name FROM fleet_hiring WHERE city_id = 1')
        etm_record = cur.fetchall()
        etm_df = pd.DataFrame(etm_record)
        etm = etm_df.set_axis(['id', 'name', 'mobile', 'city_id', 'driver_id', 'location_id', 'marital_status', 'age', 'shift', 'source', 'reference', 'other_source_reference', 'uber_id_status', 'uber_id_detailed_status', 'trainer_name', 'training_date', 'status', 'hr_date', 'onboarding_date', 'test_date', 'last_driven_partner_name', 'last_driven_partner_name'], axis='columns')
        
        cur.execute('SELECT id, name, mobile, city_id, driver_id, location_id, marital_status, age, shift, source, reference, other_source_reference, uber_id_status, uber_id_detailed_status, trainer_name, training_date, status, hr_date, onboarding_date, test_date, last_driven_partner_name, last_driven_partner_name FROM fleet_hiring WHERE city_id = 2')
        etb_record = cur.fetchall()
        etb_df = pd.DataFrame(etb_record)
        etb = etb_df.set_axis(['id', 'name', 'mobile', 'city_id', 'driver_id', 'location_id', 'marital_status', 'age', 'shift', 'source', 'reference', 'other_source_reference', 'uber_id_status', 'uber_id_detailed_status', 'trainer_name', 'training_date', 'status', 'hr_date', 'onboarding_date', 'test_date', 'last_driven_partner_name', 'last_driven_partner_name'], axis='columns')
        
        cur.execute('SELECT id, name, mobile, city_id, driver_id, location_id, marital_status, age, shift, source, reference, other_source_reference, uber_id_status, uber_id_detailed_status, trainer_name, training_date, status, hr_date, onboarding_date, test_date, last_driven_partner_name, last_driven_partner_name FROM fleet_hiring WHERE city_id = 3')
        etn_record = cur.fetchall()
        etn_df = pd.DataFrame(etn_record)
        etn = etn_df.set_axis(['id', 'name', 'mobile', 'city_id', 'driver_id', 'location_id', 'marital_status', 'age', 'shift', 'source', 'reference', 'other_source_reference', 'uber_id_status', 'uber_id_detailed_status', 'trainer_name', 'training_date', 'status', 'hr_date', 'onboarding_date', 'test_date', 'last_driven_partner_name', 'last_driven_partner_name'], axis='columns')
        
        cur.execute('SELECT id, name, mobile, city_id, driver_id, location_id, marital_status, age, shift, source, reference, other_source_reference, uber_id_status, uber_id_detailed_status, trainer_name, training_date, status, hr_date, onboarding_date, test_date, last_driven_partner_name, last_driven_partner_name FROM fleet_hiring WHERE city_id = 4')
        eth_record = cur.fetchall()
        eth_df = pd.DataFrame(eth_record)
        eth = eth_df.set_axis(['id', 'name', 'mobile', 'city_id', 'driver_id', 'location_id', 'marital_status', 'age', 'shift', 'source', 'reference', 'other_source_reference', 'uber_id_status', 'uber_id_detailed_status', 'trainer_name', 'training_date', 'status', 'hr_date', 'onboarding_date', 'test_date', 'last_driven_partner_name', 'last_driven_partner_name'], axis='columns')
        
        cur.execute('SELECT id, name, mobile, city_id, driver_id, location_id, marital_status, age, shift, source, reference, other_source_reference, uber_id_status, uber_id_detailed_status, trainer_name, training_date, status, hr_date, onboarding_date, test_date, last_driven_partner_name, last_driven_partner_name FROM fleet_hiring WHERE city_id = 5')
        etp_record = cur.fetchall()
        etp_df = pd.DataFrame(etp_record)
        etp = etp_df.set_axis(['id', 'name', 'mobile', 'city_id', 'driver_id', 'location_id', 'marital_status', 'age', 'shift', 'source', 'reference', 'other_source_reference', 'uber_id_status', 'uber_id_detailed_status', 'trainer_name', 'training_date', 'status', 'hr_date', 'onboarding_date', 'test_date', 'last_driven_partner_name', 'last_driven_partner_name'], axis='columns')
        
        print("Converted db to Fleet_hiring dataframe succesfully")
        
        #Creating Connection for google sheet
        
        client = pygsheets.authorize(service_file='client_secret.json')
        
        #pushing to sheets
        
        sheet= client.open_by_key('1xQlorzAmjDzKVLGalIzxWaC3LnwMPNPaZz6oLLWPQ6M')#Fleet_driver_2
        ws= sheet.worksheet_by_title('Fleet_hiring')
        ws.clear(start='A',end='V')
        ws.set_dataframe(Fleet_driver_2,(1,1))
  
        sheet= client.open('mumbai_driver')
        ws= sheet.worksheet_by_title('Fleet_hiring')
        ws.clear(start='A',end='V')
        ws.set_dataframe(etm,(1,1))
        
        sheet= client.open('bangalore_driver')
        ws= sheet.worksheet_by_title('Fleet_hiring')
        ws.clear(start='A',end='V')
        ws.set_dataframe(etb,(1,1))
        
        sheet= client.open('newdelhi_driver')
        ws= sheet.worksheet_by_title('Fleet_hiring')
        ws.clear(start='A',end='V')
        ws.set_dataframe(etn,(1,1))
        
        sheet= client.open('hyderabad_driver')
        ws= sheet.worksheet_by_title('Fleet_hiring')
        ws.clear(start='A',end='V')
        ws.set_dataframe(eth,(1,1))
        
        sheet= client.open('pune_driver')
        ws= sheet.worksheet_by_title('Fleet_hiring')
        ws.clear(start='A',end='V')
        ws.set_dataframe(etp,(1,1))
        
        print("Fleet_hiring Sheet Updated Succesfully")

except Error as e:
    print("Error:",e)
        
# Fleet_car

try:
    dbconnect = MySQLdb.connect(host = localhost, user = pythonuser, password = pythonpwd123, database = database)
    
    if dbconnect.get_server_info():
        print("Connected to MySQL Server, version is",dbconnect.get_server_info())
        
        #getting from database
        
        cur = dbconnect.cursor()
        
        cur.execute('SELECT id, car_number, city_id, model_id, purchase_date, rc_expiry, insurance_expiry, permit_expiry, permit_authriztion, is_deleted, manager_id, is_active FROM fleet_car')
        Fleet_driver_2_record = cur.fetchall()
        Fleet_driver_2_df = pd.DataFrame(Fleet_driver_2_record)
        Fleet_driver_2 = Fleet_driver_2_df.set_axis(['id', 'car_number', 'city_id', 'model_id', 'purchase_date', 'rc_expiry', 'insurance_expiry', 'permit_expiry', 'permit_authriztion', 'is_deleted', 'manager_id', 'is_active'], axis='columns')
       
        cur.execute('SELECT id, car_number, city_id, model, purchase_date, rc_expiry, insurance_expiry, permit_expiry, permit_authriztion, is_deleted, is_active FROM fleet_car WHERE city_id = 1 AND is_active = 1')
        etm_record = cur.fetchall()
        etm_df = pd.DataFrame(etm_record)
        etm = etm_df.set_axis(['id', 'car_number', 'city_id', 'model', 'purchase_date', 'rc_expiry', 'insurance_expiry', 'permit_expiry', 'permit_authriztion', 'is_deleted', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, car_number, city_id, model, purchase_date, rc_expiry, insurance_expiry, permit_expiry, permit_authriztion, is_deleted, is_active FROM fleet_car WHERE city_id = 2 AND is_active = 1')
        etb_record = cur.fetchall()
        etb_df = pd.DataFrame(etb_record)
        etb = etb_df.set_axis(['id', 'car_number', 'city_id', 'model', 'purchase_date', 'rc_expiry', 'insurance_expiry', 'permit_expiry', 'permit_authriztion', 'is_deleted', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, car_number, city_id, model, purchase_date, rc_expiry, insurance_expiry, permit_expiry, permit_authriztion, is_deleted, is_active FROM fleet_car WHERE city_id = 3 AND is_active = 1')
        etn_record = cur.fetchall()
        etn_df = pd.DataFrame(etn_record)
        etn = etn_df.set_axis(['id', 'car_number', 'city_id', 'model', 'purchase_date', 'rc_expiry', 'insurance_expiry', 'permit_expiry', 'permit_authriztion', 'is_deleted', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, car_number, city_id, model, purchase_date, rc_expiry, insurance_expiry, permit_expiry, permit_authriztion, is_deleted, is_active FROM fleet_car WHERE city_id = 4 AND is_active = 1')
        eth_record = cur.fetchall()
        eth_df = pd.DataFrame(eth_record)
        eth = eth_df.set_axis(['id', 'car_number', 'city_id', 'model', 'purchase_date', 'rc_expiry', 'insurance_expiry', 'permit_expiry', 'permit_authriztion', 'is_deleted', 'is_active'], axis='columns')
        
        cur.execute('SELECT id, car_number, city_id, model, purchase_date, rc_expiry, insurance_expiry, permit_expiry, permit_authriztion, is_deleted, is_active FROM fleet_car WHERE city_id = 5 AND is_active = 1')
        etp_record = cur.fetchall()
        etp_df = pd.DataFrame(etp_record)
        etp = etp_df.set_axis(['id', 'car_number', 'city_id', 'model', 'purchase_date', 'rc_expiry', 'insurance_expiry', 'permit_expiry', 'permit_authriztion', 'is_deleted', 'is_active'], axis='columns')
        
        print("Converted db to Fleet_car dataframe succesfully")
        
        #Creating Connection for google sheet
        
        client = pygsheets.authorize(service_file='client_secret.json')
        
        #pushing to sheets
                    
        sheet= client.open_by_key('1xQlorzAmjDzKVLGalIzxWaC3LnwMPNPaZz6oLLWPQ6M')#Fleet_driver_2
        ws= sheet.worksheet_by_title('Fleet_car')
        ws.clear()
        ws.set_dataframe(Fleet_driver_2,(1,1))

        sheet= client.open('mumbai_driver')
        ws= sheet.worksheet_by_title('Fleet_car')
        ws.clear()
        ws.set_dataframe(etm,(1,1))
        
        sheet= client.open('bangalore_driver')
        ws= sheet.worksheet_by_title('Fleet_car')
        ws.clear()
        ws.set_dataframe(etb,(1,1))
        
        sheet= client.open('newdelhi_driver')
        ws= sheet.worksheet_by_title('Fleet_car')
        ws.clear()
        ws.set_dataframe(etn,(1,1))
        
        sheet= client.open('hyderabad_driver')
        ws= sheet.worksheet_by_title('Fleet_car')
        ws.clear()
        ws.set_dataframe(eth,(1,1))
        
        sheet= client.open('pune_driver')
        ws= sheet.worksheet_by_title('Fleet_car')
        ws.clear()
        ws.set_dataframe(etp,(1,1))
        
        print("Fleet_car Sheet Updated Succesfully")

except Error as e:
    print("Error:",e)