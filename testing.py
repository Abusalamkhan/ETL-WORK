#importing libraries

import pandas as pd
import warnings
warnings.filterwarnings("ignore")
from openpyxl import load_workbook
import mysql.connector
from mysql.connector import Error

df = pd.read_excel (r'C:\Users\sagar\Box\FLEET DATA\salam\uber fleet name & password.xlsx',sheet_name='Sheet1')
df = df[["CITY","Narration name in tally"]]
lst=['6278']
for index, row in df.iterrows():
    if row["CITY"]:
        lst.append(row["Narration name in tally"])
tabs=[]
tabs = ([str(elem) for elem in lst])
print("Tabs contains: ",tabs)

fleet_list=[]
try:
    for i in tabs:
        if i in tabs:
            try:
                sheet_name=""+i+""
                ws='C:\\Users\\sagar\\Box\\FLEET DATA\\salam\\salam all sheets\\'+sheet_name+'.xlsx' #changes
                df = pd.read_excel(ws,sheet_name=0)#changes
                duplicated_columns_list = []
                list_of_all_columns = list(df.columns)
                for column in list_of_all_columns:
                    if list_of_all_columns.count(column) > 1 and not column in duplicated_columns_list:
                        duplicated_columns_list.append(column)
                        duplicated_columns_list
                for column in duplicated_columns_list:
                    list_of_all_columns[list_of_all_columns.index(column)] = column + '_1'
                    list_of_all_columns[list_of_all_columns.index(column)] = column + '_2'
                    list_of_all_columns[list_of_all_columns.index(column)] = column + '_3'

                df.columns = list_of_all_columns
#                 print(df.columns)
                
                
                df_unique=df['Org alias'].unique()
                print("Fleet name :",df_unique,"and code is",i)
                print("\n")
                
                columns = pd.read_excel (r'C:\Users\sagar\Box\FLEET DATA\salam\uber fleet name & password.xlsx',sheet_name='columns')
                columns_list=list(columns['columns'])
                for col in columns_list:
                    pty_columns = ''
                    try:
                        pty_columns=df[col].sum()
                    except KeyError:
                        pty_columns = 0
                    print(col,pty_columns)

                    fleet_dict={

                    "Fleet code":i,
                    "Fleet name " :df_unique,
                    col: pty_columns
                    }
                print(fleet_dict)

                fleet_list.append(fleet_dict)
                
                
            except:
                print(i,"sheet not found")            
except Error as e:
    print(e,"Some error occured")
    
df_list = pd.DataFrame(fleet_list)
df_list
# dfm=df_list.T
# dfm