
import pandas as pd
import pygsheets
import warnings
warnings.filterwarnings("ignore")


clients = pygsheets.authorize(service_file='car-master-sheet.json')
uber_ws='C:\\Users\\sagar\\Dropbox\\Dropbox\\DM Dashboard\\Master View.xlsx' 
wtd_sheet='1E9U_nG61vCYxbc3yQJHEKpIPfYuQxolllthf8weEckU'

wtd_df= pd.read_excel(uber_ws,sheet_name=-1)#uber sheet
print(wtd_df.head(10))
wtd_df=wtd_df[['Total Trips','Car No']]
sheet= clients.open_by_key(wtd_sheet)
ws= sheet.worksheet_by_title('Trips_Data')
ws.set_dataframe(wtd_df,(1,1))