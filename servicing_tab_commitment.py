#importing libraries

import pandas as pd
import datetime
from datetime import date, timedelta
import pygsheets
import warnings
warnings.filterwarnings("ignore")

#creating connections

clients = pygsheets.authorize(service_file='repair.json')

#sheet keys

commitment_mapping_3='1sy3Gxrnh8bX6ibpGR6X8rba8H2IfeSNdBGT7CIFi3SY'
servicing_calling_mumbai='1tdBXqs_kSEfl1sGk9gEEEUSgRcCTqAUDw7r13-7FmzA'

#5 sheets keys

terrific='1f-DJ5O3zKKkAtXfXQghWzbsoXyy-ipdJ5q7Yma4tF9M'
roaring='1MiZZY9MPEhhg-B9LyfmThQGV3Dy3XnKpb5NqgaYFwIQ'
silent='1ZwXyZPyt7qhjTO5VkfwoGTJ48fmg_38MTdy4EHdVhwA'
deep='1TPRKPsQfy4qY19byAuYi0Ie3Xld7ddrrBS8xqhggtcM'
black='1KD0ABDWp3YvqIkmoQAZKRS-7-S8i-HtlO4KKpBMB1tU'


sheet= clients.open_by_key(servicing_calling_mumbai)
ws= sheet.worksheet_by_title('Confirm Appointment List (DM)')
data = ws.get_all_values()
headers = data.pop(2)
master_servicing_df= pd.DataFrame(data,columns=headers)
master_servicing=master_servicing_df[master_servicing_df['DM Name'].isin(['Deep Hunters', 'Silent Killers', 'Terrific Tigers', 'Black Panthers', 'Roaring Lions'])]
master_servicing

sheet= clients.open_by_key(commitment_mapping_3)
ws= sheet.worksheet_by_title('Servicing')
ws.clear(start='A',end='L')
ws.set_dataframe(master_servicing,(1,1))

terific_servicing = master_servicing[master_servicing["DM Name"].isin(["Terrific Tigers"])]
sheet= clients.open_by_key(terrific)
ws= sheet.worksheet_by_title('Servicing')
ws.clear(start='A',end='L')
ws.set_dataframe(terific_servicing,(1,1))

roaring_servicing = master_servicing[master_servicing["DM Name"].isin(["Roaring Lions"])]
sheet= clients.open_by_key(roaring)
ws= sheet.worksheet_by_title('Servicing')
ws.clear(start='A',end='L')
ws.set_dataframe(roaring_servicing,(1,1))

silent_servicing = master_servicing[master_servicing["DM Name"].isin(["Silent Killers"])]
sheet= clients.open_by_key(silent)
ws= sheet.worksheet_by_title('Servicing')
ws.clear(start='A',end='L')
ws.set_dataframe(silent_servicing,(1,1))

deep_servicing = master_servicing[master_servicing["DM Name"].isin(["Deep Hunters"])]
sheet= clients.open_by_key(deep)
ws= sheet.worksheet_by_title('Servicing')
ws.clear(start='A',end='L')
ws.set_dataframe(deep_servicing,(1,1))

black_servicing = master_servicing[master_servicing["DM Name"].isin(["Black Panthers"])]
sheet= clients.open_by_key(black)
ws= sheet.worksheet_by_title('Servicing')
ws.clear(start='A',end='L')
ws.set_dataframe(black_servicing,(1,1))