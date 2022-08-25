import gspread
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")
scope= ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials= ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 
client= gspread.authorize(credentials)
sheet= client.open('Allotment data for Commitment ')
ws= sheet.worksheet('Master')
data = ws.get_all_values()
sheet= client.open('Commitement Mapping')
ws= sheet.worksheet('Allotment Query data')
ws.update(data, value_input_option='USER_ENTERED')
print("Updated successfully")