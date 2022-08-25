#importing libraries
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
mumbai=['6278','278','620','877','108','11','12','13','Fourteen Fleet', '9 Fleet', 'seedan Fleet', '982','776','981','816','455','788','93']
delhi=['DEL-1', 'DEL-2']
bangalore=['Pramod','OP.BLR','Everest BLR -3','Everest BLR-4','Everest BLR-5']
hyderabad=['Everest HYD-1','Everest HYD-2','Hyd', 'Hyd leasing -1','Hyd leasing -2', 'Everest HYD Leasing- 4']
pune=['Pune-1']
# ws_input=input("Enter your file path :")
# ws= "" + ws_input + ""
inp=input("enter city name : ")
tabs=[]
if 'delhi' in inp:
    tabs.extend(delhi)
    print("delhi tab contains : ",tabs)
elif 'mumbai' in inp:
    tabs.extend(mumbai)
    print("mumbai tab contains : ",tabs)
elif 'bangalore' in inp:
    tabs.extend(bangalore)
    print("bangalore tab contains : ",tabs)
elif 'hyderabad' in inp:
    tabs.extend(hyderabad)
    print("hyderabad tab contains : ",tabs)
elif 'pune' in inp:
    tabs.extend(pune)
    print("pune tab contains : ",tabs)
else:
    print("Enter correct city name")
sumofallpty=0
sumofallpty1=0
sumofallpty2=0
sumofallpty3=0
sumofallpty4=0
sumofallpty5=0
sumofallpty6=0
sumofallpty7=0
sumofallpty8=0
sumofallpty9=0
sumofallpty10=0
sumofallpty11=0
sumofallpty12=0
sumofallpty13=0
sumofallpty14=0
sumofallpty15=0
sumofallpty16=0
sumofallpty17=0
sumofallpty18=0
sumofallpty19=0
sumofallpty20=0
sumofallpty21=0
sumofallpty22=0
sumofallpty23=0
sumofallpty24=0            
try:
    for i in tabs:
        if i in tabs:
            try:
                sheet_name=""+i+""
                df = pd.read_excel(r"C:\\Users\\sagar\\Downloads\\Uber Payment transaction google sheet.xlsx",sheet_name=sheet_name)
                df_unique=df['Org alias'].unique()
                print("Fleet name :",df_unique,"and code is",i)
                print("\n")
                pty=df['Paid to you'].sum() 
                print("Paid to you",pty)
                pty1=df['Paid to you : Your earnings'].sum()
                print("Paid to you : Your earnings",pty1)
                pty2=df['Paid to you : Trip balance : Payouts : Cash Collected'].sum()
                print("Paid to you : Trip balance : Payouts : Cash Collected",pty2)
                pty3=df['Paid to you : Your earnings : Fare'].sum()
                print("Paid to you : Your earnings : Fare",pty3)
                pty4=df['Paid to you : Your earnings : Taxes'].sum()
                print("Paid to you : Your earnings : Taxes",pty4)
                pty5=df['Paid to you:Your earnings:Taxes:Income tax withholding'].sum()
                print("Paid to you:Your earnings:Taxes:Income tax withholding",pty5)
                pty6=df['Paid to you:Your earnings:Fare:Fare'].sum()
                print("Paid to you:Your earnings:Fare:Fare",pty6)
                pty7=df['Paid to you:Your earnings:Fare:Wait Time at Pick-up'].sum()
                print("Paid to you:Your earnings:Fare:Wait Time at Pick-up",pty7)
                pty8=df['Paid to you:Trip balance:Refunds:Toll'].sum()
                print("Paid to you:Trip balance:Refunds:Toll",pty8)
                pty9=df['Paid to you:Your earnings:Fare:Cancellation'].sum()
                print("Paid to you:Your earnings:Fare:Cancellation",pty9)
                pty10=df['Paid to you:Your earnings:Taxes:TDS on promotions'].sum()
                print("Paid to you:Your earnings:Taxes:TDS on promotions",pty10)
                pty11=df['Paid to you:Your earnings:Tip'].sum()
                print("Paid to you:Your earnings:Tip",pty11)
                pty12=df['Paid to you:Trip balance:Refunds:Parking charges'].sum()
                print("Paid to you:Trip balance:Refunds:Parking charges",pty12)
                pty13=df['Paid to you:Your earnings:Promotion:Promotion'].sum()
                print("Paid to you:Your earnings:Promotion:Promotion",pty13)
                pty14=df['Paid to you:Your earnings:Fare:Adjustment'].sum()
                print("Paid to you:Your earnings:Fare:Adjustment",pty14)
                pty15=df['Paid to you:Your earnings:Fare:Surge'].sum()
                print("Paid to you:Your earnings:Fare:Surge",pty15)
                pty16=df['Paid to you:Your earnings:Taxes:Tax on fare'].sum()
                print("Paid to you:Your earnings:Taxes:Tax on fare",pty16)
                pty17=df['Paid to you:Trip balance:Payouts:Transferred To Bank Account'].sum()
                print("Paid to you:Trip balance:Payouts:Transferred To Bank Account",pty17)
                pty18=df['Paid to you:Your earnings:Promotion:Quest'].sum()
                print("Paid to you:Your earnings:Promotion:Quest",pty18)
                pty19=df['Paid to you:Your earnings:Other earnings:Nighttime charges'].sum()
                print("Paid to you:Your earnings:Other earnings:Nighttime charges",pty19)
                try:
                    pty20=df['Paid to you:Your earnings:Promotion:Referral reward'].sum()
                except KeyError:
                     pty20 = 0
                print("Paid to you:Your earnings:Promotion:Referral reward",pty20)
                try:
                    pty21=df['Paid to you:Your earnings:Fare:Reservation Fee'].sum()
                except KeyError:
                     pty21 = 0
                print("Paid to you:Your earnings:Fare:Reservation Fee",pty21)
                pty22=df['Paid to you:Your earnings:Other earnings:Adjustment'].sum()
                print("Paid to you:Your earnings:Other earnings:Adjustment",pty22)
                pty23=df['Paid to you:Your earnings:Fare:Adjustment'].sum()
                print("Paid to you:Your earnings:Fare:Adjustment",pty23)
                try:
                    pty24=df['Paid to you:Your earnings:Promotion:Boost*'].sum()
                except KeyError:
                    pty24 = 0
                print("Paid to you:Your earnings:Promotion:Boost*",pty24)
                print("\n")
                sumofallpty = sumofallpty+pty
                sumofallpty1 = sumofallpty1+pty1
                sumofallpty2 = sumofallpty2+pty2
                sumofallpty3 = sumofallpty3+pty3
                sumofallpty4 = sumofallpty4+pty4
                sumofallpty5 = sumofallpty5+pty5
                sumofallpty6 = sumofallpty6+pty6
                sumofallpty7 = sumofallpty7+pty7
                sumofallpty8 = sumofallpty8+pty8
                sumofallpty9 = sumofallpty9+pty9
                sumofallpty10 = sumofallpty10+pty10
                sumofallpty11 = sumofallpty11+pty11
                sumofallpty12 = sumofallpty12+pty12
                sumofallpty13 = sumofallpty13+pty13
                sumofallpty14 = sumofallpty14+pty14
                sumofallpty15 = sumofallpty15+pty15
                sumofallpty16 = sumofallpty16+pty16
                sumofallpty17 = sumofallpty17+pty17
                sumofallpty18 = sumofallpty18+pty18
                sumofallpty19 = sumofallpty19+pty19
                sumofallpty20 = sumofallpty20+pty20
                sumofallpty21 = sumofallpty21+pty21
                sumofallpty22 = sumofallpty22+pty22
                sumofallpty23 = sumofallpty23+pty23
                sumofallpty24 = sumofallpty24+pty24
            except:
                print(i,"tab not found")
    print("Sum of above fleets are :")
    print("Total Paid to you",sumofallpty)
    print("Total Paid to you : Your earnings",sumofallpty1)
    print("Total Paid to you : Trip balance : Payouts : Cash Collected",sumofallpty2)
    print("Total Paid to you : Your earnings : Fare",sumofallpty3)
    print("Total Paid to you : Your earnings : Taxes",sumofallpty4)
    print("Total Paid to you:Your earnings:Taxes:Income tax withholding",sumofallpty5)
    print("Total Paid to you:Your earnings:Fare:Fare",sumofallpty6)
    print("Total Paid to you:Your earnings:Fare:Wait Time at Pick-up",sumofallpty7)
    print("Total Paid to you:Trip balance:Refunds:Toll",sumofallpty8)
    print("Total Paid to you:Your earnings:Fare:Cancellation",sumofallpty9)
    print("Total Paid to you:Your earnings:Taxes:TDS on promotions",sumofallpty10)
    print("Total Paid to you:Your earnings:Tip",sumofallpty11)
    print("Total Paid to you:Trip balance:Refunds:Parking charges",sumofallpty12)
    print("Total Paid to you:Your earnings:Promotion:Promotion",sumofallpty13)
    print("Total Paid to you:Your earnings:Fare:Adjustment",sumofallpty14)
    print("Total Paid to you:Your earnings:Fare:Surge",sumofallpty15)
    print("Total Paid to you:Your earnings:Taxes:Tax on fare",sumofallpty16)
    print("Total Paid to you:Trip balance:Payouts:Transferred To Bank Account",sumofallpty17)
    print("Total Paid to you:Your earnings:Promotion:Quest",sumofallpty18)
    print("Total Paid to you:Your earnings:Other earnings:Nighttime charges",sumofallpty19)
    print("Total Paid to you:Your earnings:Promotion:Referral reward",sumofallpty20)
    print("Total Paid to you:Your earnings:Fare:Reservation Fee",sumofallpty21)
    print("Total Paid to you:Your earnings:Other earnings:Adjustment",sumofallpty22)
    print("Total Paid to you:Your earnings:Fare:Adjustment",sumofallpty23)
    print("Total Paid to you:Your earnings:Promotion:Boost*",sumofallpty24)
                
except:
    print("some error occured")