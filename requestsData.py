import requests
import pandas as pd
import numpy as np
import requests
import datetime
import time
import random
import sqlite3


class SQLError(BaseException):
    pass

def Sqlconnection():
    conn = sqlite3.connect("D:\SQLLite_Stock\TW_Stock.db")
    c=conn.cursor()
    return c,conn

def SQLData_Table_Check(stock_no):
    c,conn=Sqlconnection()
    try:
        strSql="select * from t"+stock_no
        c.execute(strSql)
        print('has DataTable')

    except:
        # c,conn=Sqlconnection()
        print('No DataTable')
        strsql="CREATE TABLE t"+stock_no+" (DATEDAY TEXT,DATEMONTH TEXT, STOCKNO CHAR(10), STOCKNAME TEXT, TRADE_VOL INT, TRANS_ACTION INT, TRADE_VALUE INT, OPEN_PRICE FLOAT, HIGH_PRICE FLOAT, LOW_PRICE FLOAT, CLOSE_PRICE FLOAT, DIR TEXT, CHANG FLOAT, PE_RATIO FLOAT)"
        c.execute(strsql)
        conn.commit()

def SQLData_Info_Check(date, stock_no,row):
    c,conn=Sqlconnection()
    strsql="select DATEDAY from t"+stock_no+" where DATEDAY='"+date+"'"
    c.execute(strsql)
    checkdata=c.fetchone()
    if checkdata==None:
        strfield="insert into t"+stock_no+" (DATEDAY,DATEMONTH,STOCKNO,STOCKNAME,TRADE_VOL,TRANS_ACTION,TRADE_VALUE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,DIR,CHANG,PE_RATIO) values("
        strvalue="'"+date+"','"+date[:6]+"',"
        for row_column in range(len(row)):
            if row_column==9:
                # print(row[row_column].split(">")[1].replace("</p",""))
                strvalue+="'"+row[row_column].split(">")[1].replace("</p","")+"',"
            elif row_column!=11 and row_column!=12 and row_column!=13 and row_column!=14:
                if row_column==15:
                    strvalue+="'"+row[row_column]+"')"
                else:
                    strvalue+="'"+row[row_column]+"',"
        print(strfield+strvalue)
        c.execute(strfield+strvalue)
        conn.commit()

#加權指數確認日期有無資料
def TA00_Check_Date(date):
    c,conn=Sqlconnection()
    strsql="SELECT DATEDAY t where DATEDAY='"+date+"'"
    c.execute(strsql)
    checkdate=c.fetchone()
    if checkdate==None:
        return True
    else:
        return False


def get_stock_histor(date):
    # date ='20210309'
    url1 = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=ALLBUT0999"%(date)
    r = requests.get(url1)
    # r.text
    #'證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額',       '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)',                    '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比'
    # 0050       元大台灣50  25,312,461  27,821     3,593,629,562  142.60     142.80    141.20    142.30    <p style= color:red>+</p>      0.75       142.30         108            142.35         400            0.00
    stockdata = r.json()
    if stockdata['stat'] == '很抱歉，沒有符合條件的資料!':
        pass
    else:
        print(pd.DataFrame(stockdata['data9'], columns = stockdata['fields9'])) # data4
        print(stockdata['fields9'])
        for row in stockdata['data9']:
            print(row) # to sql
            stock_no=row[0].replace("'","")
            SQLData_Table_Check(stock_no)
            SQLData_Info_Check(date,stock_no,row)
    

def today_stock():
    loc_dt = datetime.datetime.today() 
    
    time_del = datetime.timedelta(days=3) 
    loc_dt = loc_dt - time_del 
    # print(loc_dt.isoweekday())
    if loc_dt.isoweekday() < 6:
        loc_dt_format = loc_dt.strftime("%Y%m%d") # %H:%M:%S
        get_stock_histor(loc_dt_format)
        return loc_dt_format


def Init():
    loc_dt = datetime.datetime.today() 
    for i in range(1,365):
        time_del = datetime.timedelta(days=i) 
        new_dt = loc_dt - time_del 
        print(new_dt.isoweekday())
        if new_dt.isoweekday() <6 : # to fix sql已存在
            new_dt_format = new_dt.strftime("%Y%m%d") # %H:%M:%S
            if TA00_Check_Date(new_dt_format)==True:
                print(new_dt_format)
                t = random.randrange(1500,6000) # wait
                # print(t)
                time.sleep(t/1000)
                get_stock_histor(new_dt_format)


# print(today_stock())
c,conn=Sqlconnection()
Init()
today_stock()