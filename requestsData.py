
import requests
import pandas as pd
import numpy as np
import requests
import datetime
import time
import random
import sqlite3
from io import StringIO


def Sqlconnection():
    conn = sqlite3.connect("StockQ.db")
    c = conn.cursor()
    return c, conn


def Stock_DB_Connection():
    conn2 = sqlite3.connect("Stock_ID.db")
    c2 = conn2.cursor()
    return c2, conn2

def check_database_max_date():
    c,conn=Sqlconnection()
    strsql="select max(DATEDAY) from TWSE_DAY"
    c.execute(strsql)
    result=c.fetchone()
    return result


def SQL_CreateTable():
    c, conn = Sqlconnection()
    # 上市                                     證券代號","證券名稱","成交股數","成交筆數","成交金額","開盤價","最高價","最低價","收盤價","漲跌(+/-)","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比"
    strsql = "CREATE TABLE TWSE_DAY (DATEDAY DATE, STOCKNO CHAR(10), STOCKNAME TEXT, TRADE_VOL FLOAT, TRADE_VALUE FLOAT, OPEN_PRICE FLOAT, HIGH_PRICE FLOAT, LOW_PRICE FLOAT, CLOSE_PRICE FLOAT, AVG_A_VALUE FLOAT)"
    c.execute(strsql)
    # add index
    strsql = "CREATE INDEX DATEDAY_index on TWSE_DAY (DATEDAY)"
    c.execute(strsql)
    strsql = "CREATE INDEX STOCKNO_index on TWSE_DAY (STOCKNO)"
    c.execute(strsql)
    conn.commit()


def SQL_SB_CreateTable():
    c, conn = Sqlconnection()
    strsql = """CREATE TABLE BS_INFO (DATETIME DATE, 
                                    STOCKID CHAR(10), 
                                    STOCKNAME TEXT, 
                                    QFVOL_BUY FLOAT, 
                                    QFVOL_SELL FLOAT, 
                                    QFVOL_GAP FLOAT, 
                                    TRUSTVOL_BUY FLOAT, 
                                    TRUSTVOL_SELL FLOAT, 
                                    TRUSTVOL_GAP FLOAT, 
                                    DEALERVOL_BUY FLOAT, 
                                    DEALERVOL_SELL FLOAT, 
                                    DEALERVOL_GAP FLOAT)"""
    c.execute(strsql)
    strsql = "CREATE INDEX DATETIME_index on BS_INFO (DATETIME)"
    c.execute(strsql)
    strsql = "CREATE INDEX STOCKID_index on BS_INFO (STOCKID)"
    c.execute(strsql)
    conn.commit()

#融資融券明細
def create_Maring_DB():
    c,conn=Sqlconnection()
    strsql="""CREATE TABLE MARGIN_LIST (MARGIN_DATE DATE,
                                        MARGIN_ID CHAR(10),
                                        MARGIN_NAME TEXT,
                                        MARGIN_FIN_BUY FLOAT,
                                        MARGIN_FIN_SELL FLOAT,
                                        MARGIN_FIN_BEF_VOL FLOAT,
                                        MARGIN_FIN_NOW_VOL FLOAT,
                                        MARGIN_FIN_GUOTA_VOL FLOAT,
                                        MARGIN_SEC_BUY FLOAT,
                                        MARGIN_SEC_SELL FLOAT,
                                        MARGIN_SEC_BEF_VOL FLOAT,
                                        MARGIN_SEC_NOW_VOL FLOAT,
                                        MARGIN_SEC_GUOTA_VOL FLOAT) """

    c.execute(strsql)
    strsql="CREATE INDEX MARGIN_DATE_INDEX ON MARGIN_LIST(MARGIN_DATE)"
    c.execute(strsql)
    strsql="CREATE INDEX MARGIN_ID_INDEX ON MARGIN_LIST (MARGIN_ID)"
    c.execute(strsql)
    conn.commit()

# 上市上櫃當沖明細
def SQL_DC_CreateTable():
    c, conn = Sqlconnection()
    strsql = """CREATE TABLE DC_DAYLIST (DC_DATE DATE,
                                    DC_STOCKID CHAR(10),
                                    DC_STOCKNAME TEXT,
                                    DC_VOL FLOAT,
                                    DC_INPUT_VALUE FLOAT,
                                    DC_OUTPUT_VALUE FLOAT)"""
    c.execute(strsql)
    strsql = "CREATE INDEX DCDATE_INDEX ON DC_DAYLIST (DC_DATE)"
    c.execute(strsql)
    strsql = "CREATE INDEX DC_STOCKID_INDEX ON DC_DAYLIST (DC_STOCKID)"
    c.execute(strsql)
    conn.commit()


def Stock_ID_CreateTable(stockID):
    c2, conn2 = Stock_DB_Connection()
    strsql = """create table 't{}' (DATEDAY DATE, 
                                    STOCKNO CHAR(10), 
                                    STOCKNAME TEXT, 
                                    TRADE_VOL FLOAT, 
                                    TRADE_VALUE FLOAT, 
                                    OPEN_PRICE FLOAT, 
                                    HIGH_PRICE FLOAT, 
                                    LOW_PRICE FLOAT, 
                                    CLOSE_PRICE FLOAT, 
                                    AVG_A_VALUE FLOAT,
                                    DC_VOL FLOAT,   
                                    DC_VOL_PERCENT FLOAT, 
                                    NET_GAP_VOL FLOAT,
                                    NET_GAP_VALUE FLOAT,
                                    NET_GAP_AVE_PRICE FLOAT,
                                    QFVOL_BUY FLOAT,
                                    QFVOL_SELL FLOAT,
                                    TRUSTVOL_BUY FLOAT,
                                    TRUSTVOL_SELL FLOAT,
                                    QFVOL_BUY_VALUE FLOAT,
                                    QFVOL_SELL_VALUE FLOAT,
                                    TRUSTVOL_BUY_VALUE FLOAT,
                                    TRUSTVOL_SELL_VALUE FLOAT,
                                    QF_BUY_COST_PRICE FLOAT,
                                    QF_SELL_COST_PRICE FLOAT,
                                    TRUST_BUY_COST_PRICE FLOAT,
                                    TRUST_SELL_COST_PRICE FLOAT,
                                    QF_AVG_NET_PRICE_BUY FLOAT,
                                    QF_AVG_NET_PRICE_SELL FLOAT,
                                    TRUST_AVG_NET_PRICE_BUY FLOAT,
                                    TRUST_AVG_NET_PRICE_SELL FLOAT,
                                    AM_ENERGY_VALUE FLOAT)""".format(stockID)
    c2.execute(strsql)
    conn2.commit()
    
def check_stockID_Table(StockNum):
    c2,conn2=Stock_DB_Connection()
    strsql = "SELECT * FROM sqlite_master WHERE type = 'table' AND name = 't"+str(StockNum)+"'"
    c2.execute(strsql)
    checkTable=c2.fetchall()
    if checkTable==[]:
        Stock_ID_CreateTable(StockNum)
    


def twse_check_date_bool(date):
    c, conn = Sqlconnection()
    strsql = "select DATEDAY FROM TWSE_DAY where DATEDAY='"+date+"'"
    try:
        c.execute(strsql)
        checkdate = c.fetchone()
        conn.commit()
        if checkdate == None:
            return True
        else:
            return False
    except:
        return True


def check_buy_sell_bool(date):
    c, conn = Sqlconnection()
    strsql = "select DATETIME FROM BS_INFO where DATETIME='"+date+"'"
    try:
        c.execute(strsql)
        checkdate = c.fetchone()
        if checkdate == None:
            return True
        else:
            return False

    except:
        return True


def check_DC_date(date):
    c,conn = Sqlconnection()
    strsql ="select DC_DATE FROM DC_DAYLIST where DC_DATE='"+date+"'"
    try:
        c.execute(strsql)
        checkdate=c.fetchone()
        if checkdate==None:
            return True
        else:
            return False
    except:
        return True

# 加權指數確認日期有無資料
# def TA00_Check_Date(date):
#     c,conn=Sqlconnection()
#     strsql="SELECT DATEDAY TWII where DATEDAY='"+date+"'"
#     c.execute(strsql)
#     checkdate=c.fetchone()
#     if checkdate==None:
#         return True
#     else:
#         return False

# 上市上櫃股價
def get_stock_histor_twse(date, db_date, otc_dt_format):
    # date ='20210309'
    global r
    try:
        url1 = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=ALLBUT0999" % (
            date)
        r = requests.get(url1)
    except:
        ta = random.randrange(1500, 6000)  # wait
        time.sleep(ta/1000)
        return r
    # r.text
    #'證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額',       '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)',                    '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比'
    # 0050       元大台灣50  25,312,461  27,821     3,593,629,562  142.60     142.80    141.20    142.30    <p style= color:red>+</p>      0.75       142.30         108            142.35         400            0.00
    stockdata = r.json()
    if stockdata['stat'] == '很抱歉，沒有符合條件的資料!':
        pass
    else:
        try:
            # print(pd.DataFrame(stockdata['data9'], columns = stockdata['fields9'])) # data4
            rowsdata = pd.DataFrame(
                stockdata['data9'], columns=stockdata['fields9'])
        except:
            # print(pd.DataFrame(stockdata['data8'], columns = stockdata['fields8'])) # data4
            rowsdata = pd.DataFrame(
                stockdata['data8'], columns=stockdata['fields8'])

        c, conn = Sqlconnection()
        # 集合
        new_rowdata = rowsdata[['證券代號', '證券名稱', '成交股數',
                                '成交金額', '開盤價', '最高價', '最低價', '收盤價']]
        finaldata = new_rowdata.loc[new_rowdata['證券代號'].str.len() == 4].reset_index(
            drop=True)
        finaldata.insert(0, 'DATEDAY', db_date)
        finaldata['成交股數'] = finaldata['成交股數'].str.replace(
            ',', '').astype(float)
        finaldata['成交金額'] = finaldata['成交金額'].str.replace(
            ',', '').astype(float)
        finaldata.loc[:, '每股均價'] = finaldata['成交金額']/finaldata['成交股數']
        finaldata['每股均價'] = finaldata['每股均價'].fillna(0.0).round(2)
        finaldata.columns = ['DATEDAY', 'STOCKNO', 'STOCKNAME', 'TRADE_VOL', 'TRADE_VALUE',
                             'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'AVG_A_VALUE']
        print(finaldata)

        finaldata.to_sql('TWSE_DAY', con=conn, if_exists='append', index=False)

        conn.commit()
        conn.close()
        del rowsdata, new_rowdata, finaldata

        # 上櫃
    global resp
    try:
        url2 = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d=%s&se=EW&s=0,asc,0" % (
            otc_dt_format)
        resp = requests.get(url2)
    except:
        ta = random.randrange(1500, 6000)  # wait
        time.sleep(ta/1000)
        return resp

    stockdata = resp.json()
    if stockdata['iTotalRecords'] == 0:
        pass
    else:
        try:
            # print(pd.DataFrame(stockdata['data9'], columns = stockdata['fields9'])) # data4
            rowsdata = pd.DataFrame(stockdata['aaData'], columns=['代號', '名稱', '收盤', '漲跌', '開盤', '最高',
                                    '最低', '成交股數', '成交金額', '成交筆數', '最後買價', '最後買量', '最後賣價', '最後賣量', '發行股數', '次日漲停價', '次日跌停價'])
        except:
            # print(pd.DataFrame(stockdata['data8'], columns = stockdata['fields8'])) # data4
            rowsdata = pd.DataFrame(stockdata['aaData'], columns=['代號', '名稱', '收盤', '漲跌', '開盤', '最高',
                                    '最低', '成交股數', '成交金額', '成交筆數', '最後買價', '最後買量', '最後賣價', '最後賣量', '發行股數', '次日漲停價', '次日跌停價'])

        c, conn = Sqlconnection()
        # 篩選
        new_rowdata = rowsdata[['代號', '名稱', '成交股數',
                                '成交金額', '開盤', '最高', '最低', '收盤']]
        finaldata2 = new_rowdata.loc[(new_rowdata['代號'].str.len() == 4) & (
            new_rowdata['開盤'] != '----')].reset_index(drop=True)
        finaldata2.insert(0, 'DATEDAY', db_date)
        finaldata2['成交股數'] = finaldata2['成交股數'].str.replace(
            ',', '').astype(float)
        finaldata2['成交金額'] = finaldata2['成交金額'].str.replace(
            ',', '').astype(float)
        finaldata2.loc[:, '每股均價'] = finaldata2['成交金額']/finaldata2['成交股數']
        finaldata2['每股均價'] = finaldata2['每股均價'].fillna(0.0).round(2)
        finaldata2.columns = ['DATEDAY', 'STOCKNO', 'STOCKNAME', 'TRADE_VOL', 'TRADE_VALUE',
                              'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'AVG_A_VALUE']
        print(finaldata2)

        finaldata2.to_sql('TWSE_DAY', con=conn,
                          if_exists='append', index=False)

        conn.commit()
        conn.close()

        del rowsdata, new_rowdata, finaldata2

    #  代號	名稱	收盤	漲跌	開盤	最高	最低	成交股數	成交金額(元)	成交筆數	最後買價	最後買量(千股)	最後賣價	最後賣量(千股)	發行股數	次日漲停價	次日跌停價
    #  1336	台翰	19.15	+0.05	19.45	19.50	18.90	14,000	    268,350	       10	      18.90	         2	        19.00	        1	      77,098,419	21.05	17.25

# 三大法人買賣超 股數 上市


def Legal_Buy_Sell_infomation(date, db_date):
    global r
    try:
        url = "https://www.twse.com.tw/fund/T86?response=csv&date=%s&selectType=ALL" % (
            date)
        r = requests.get(url)
    except:
        ta = random.randrange(1500, 6000)  # wait
        time.sleep(ta/1000)
        return r

    try:
        stockdata = pd.read_csv(StringIO(r.text), header=1, on_bad_lines='skip').dropna(
            axis=1, how='all').dropna(how='any')
        rowsdata = stockdata.loc[stockdata['證券代號'].str.len() == 4].reset_index(
            drop=True)
        rowsdata['證券名稱'] = rowsdata['證券名稱'].str.strip()
        newcolumn = []
        QF_Flag = False
        addFlag = True
        for col in rowsdata.columns:
            if '代號' in col:
                newcolumn.append(col)
            elif '名稱' in col:
                newcolumn.append(col)
            elif ('外陸資買' in col) or ('外陸資賣' in col):
                QF_Flag = True
                newcolumn.append(col)
            elif ('外資' in col) and QF_Flag == False:
                newcolumn.append(col)
            elif '投信' in col:
                newcolumn.append(col)
            elif ('自營商' in col) & ('避險' in col):
                addFlag = False
                newcolumn.append(col)
        if addFlag == True:
            newcolumn.append('自營商買進股數')
            newcolumn.append('自營商賣出股數')
            newcolumn.append('自營商買賣超股數')

        finaldata3 = rowsdata[newcolumn]
        finaldata3.insert(0, 'DATETIME', db_date)

        for col_name in range(2, len(newcolumn)):
            finaldata3[str(newcolumn[col_name])] = finaldata3[str(
                newcolumn[col_name])].str.replace(',', '').astype(float)

        print(finaldata3)
        finaldata3.columns = ['DATETIME', 'STOCKID', 'STOCKNAME', 'QFVOL_BUY', 'QFVOL_SELL', 'QFVOL_GAP',
                              'TRUSTVOL_BUY', 'TRUSTVOL_SELL', 'TRUSTVOL_GAP', 'DEALERVOL_BUY', 'DEALERVOL_SELL', 'DEALERVOL_GAP']

        c, conn = Sqlconnection()
        finaldata3.to_sql('BS_INFO', con=conn, if_exists='append', index=False)

        conn.commit()
        conn.close()
        del finaldata3, stockdata, rowsdata
    except:
        print(db_date+"   No data")


# 三大法人買賣超 上櫃
def Tepx_Buy_Sell_information(otc_date, db_date, nor_date):
    if int(nor_date) < int('20141201'):
        url3 = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_download.php?l=zh-tw&se=EW&t=D&d=%s&s=0,asc,0" % (
            otc_date)
    else:
        #       https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=106/01/16&s=0,asc
        url3 = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=%s&s=0,asc" % (
            otc_date)

    global resp
    try:
        resp = requests.get(url3)
    except:
        ta = random.randrange(1500, 6000)
        time.sleep(ta/1000)
        return resp

    try:
        stockdata = pd.read_csv(StringIO(resp.text), header=1, on_bad_lines='skip').dropna(
            axis=1, how='all').dropna(how='any')
        stockdata['代號'] = stockdata['代號'].astype(str)
        rowsdata = stockdata.loc[stockdata['代號'].str.len() == 4].reset_index(
            drop=True)
        rowsdata['名稱'] = rowsdata['名稱'].str.strip()
        otc_new_column = []

        QF_Flag = False
        Trust_Flag = False
        Deal_Flag = False
        for iCol in rowsdata.columns:
            if '代號' in iCol:
                otc_new_column.append(iCol)
            elif '名稱' in iCol:
                otc_new_column.append(iCol)
            elif ('外資及陸資(不含外資自營商)' in iCol):
                QF_Flag = True
                otc_new_column.append(iCol)
            elif QF_Flag == False and ('外資及陸資' in iCol):
                otc_new_column.append(iCol)
            elif ('投信-買進股數' in iCol) or ('投信-賣出股數' in iCol) or ('投信-買賣超股數' in iCol):
                Trust_Flag = True
                otc_new_column.append(iCol)
            elif Trust_Flag == False and ('投信' in iCol):
                otc_new_column.append(iCol)
            elif ('自營商(避險)' in iCol):
                Deal_Flag = True
                otc_new_column.append(iCol)
            elif ('自行買賣' in iCol) or ('外資自營商' in iCol):
                pass
            elif Deal_Flag == False and ('自營商' in iCol):
                otc_new_column.append(iCol)
            # elif Deal_Flag == False and ('自營' in iCol):
            #     otc_new_column.append(iCol)

        finaldata = rowsdata[otc_new_column]
        finaldata.insert(0, 'DATETIME', db_date)

        for new_col in range(2, len(otc_new_column)):
            finaldata[str(otc_new_column[new_col])] = finaldata[str(
                otc_new_column[new_col])].str.replace(',', '').astype(float)

        print(finaldata)
        finaldata.columns = ['DATETIME', 'STOCKID', 'STOCKNAME', 'QFVOL_BUY', 'QFVOL_SELL', 'QFVOL_GAP',
                             'TRUSTVOL_BUY', 'TRUSTVOL_SELL', 'TRUSTVOL_GAP', 'DEALERVOL_BUY', 'DEALERVOL_SELL', 'DEALERVOL_GAP']

        c, conn = Sqlconnection()
        finaldata.to_sql('BS_INFO', con=conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

        del finaldata, rowsdata, stockdata

    except:
        print(db_date+"  No Data")


def Twse_DC_Information(date, db_date,otc_date,TwseFlag):
    if TwseFlag=='True':
        url4 = "https://www.twse.com.tw/zh/exchangeReport/TWTB4U?response=json&date="+date
    else:
        url4="https://www.tpex.org.tw/web/stock/trading/intraday_stat/intraday_trading_stat_result.php?l=zh-tw&d=%s&s=0,asc,0&o=json"%(otc_date)

    global rest
    try:
        rest = requests.get(url4)
    except:
        dc_t = random.randrange(3000, 5000)
        time.sleep(dc_t/1000)
        return rest
    
    dcdata = rest.json()
    if TwseFlag=='True':
        if dcdata['data']!=None:
            checkcolumn=dcdata['fields']
            if len(checkcolumn)>5:
                try:
                    rowsdata=pd.DataFrame(dcdata['data'],columns=['證券代號','證券名稱','當沖註記','成交股數','買進成交金額','賣出成交金額'])
                except:
                    print(date+'  error')
                    return
            else:
                return
    else:
        if dcdata['aaData']!=None:
            try:
                rowsdata=pd.DataFrame(dcdata['aaData'],columns=['證券代號','證券名稱','當沖註記','成交股數','買進成交金額','賣出成交金額'])
            except:
                print(date+'  error')
                return

    newcolumn=['證券代號','證券名稱','成交股數','買進成交金額','賣出成交金額']
    rowsdata['證券代號']=rowsdata['證券代號'].astype(str)
    newsdata=rowsdata.loc[rowsdata['證券代號'].str.len()==4].reset_index(drop=True)
    finaldata=newsdata[newcolumn]
    for new_col in range(2,len(newcolumn)):
        finaldata[str(newcolumn[new_col])]=finaldata[str(newcolumn[new_col])].str.replace(',','').astype(float)

    finaldata.insert(0,'DCDATE',db_date)
    finaldata.columns=['DC_DATE','DC_STOCKID','DC_STOCKNAME','DC_VOL','DC_INPUT_VALUE','DC_OUTPUT_VALUE']

    print(finaldata)
    c,conn=Sqlconnection()
    finaldata.to_sql('DC_DAYLIST',con=conn,if_exists='append',index=False)

    conn.commit()
    conn.close()

    del finaldata,newsdata,rowsdata

#分類至個股並且by stock id create table
def Total_Stock_change_StockTable(date):
    c,conn=Sqlconnection()
    strsql="select STOCKNO from TWSE_DAY where DATEDAY='"+date+"'"
    c.execute(strsql)
    totalstock=c.fetchall()
    c.close()
    for item in range(len(totalstock)):
        stockid=totalstock[item][0]
        
        if stockid[0]!='0':
            check_stockID_Table(stockid)

            strsql="""select TWSE_DAY.DATEDAY,TWSE_DAY.STOCKNO,TWSE_DAY.STOCKNAME,TWSE_DAY.TRADE_VOL,
                            TWSE_DAY.TRADE_VALUE, TWSE_DAY.OPEN_PRICE, TWSE_DAY.HIGH_PRICE,TWSE_DAY.LOW_PRICE,
                            TWSE_DAY.CLOSE_PRICE, TWSE_DAY.AVG_A_VALUE, DC_DAYLIST.DC_VOL, DC_DAYLIST.DC_INPUT_VALUE, DC_DAYLIST.DC_OUTPUT_VALUE,
                            BS_INFO.QFVOL_BUY,BS_INFO.QFVOL_SELL, BS_INFO.TRUSTVOL_BUY,BS_INFO.TRUSTVOL_SELL
                            FROM TWSE_DAY 
                            INNER JOIN DC_DAYLIST ON TWSE_DAY.DATEDAY=DC_DAYLIST.DC_DATE and TWSE_DAY.STOCKNO=DC_DAYLIST.DC_STOCKID 
                            INNER JOIN BS_INFO ON TWSE_DAY.DATEDAY=BS_INFO.DATETIME and TWSE_DAY.STOCKNO=BS_INFO.STOCKID
                            WHERE TWSE_DAY.STOCKNO='{}' ORDER BY TWSE_DAY.DATEDAY ASC""".format(stockid)
            
            c,conn=Sqlconnection()
            totaldata2=pd.read_sql(strsql,conn)
            print (totaldata2)
            conn.close()
            totaldata=totaldata2.loc[totaldata2['OPEN_PRICE']!='--'].reset_index(drop=True)

            #淨成交量= 成交量 - 當沖量
            totaldata['NET_GAP_VOL']=totaldata['TRADE_VOL']-totaldata['DC_VOL']
            #當沖比率
            totaldata['DC_VOL_PERCENT']=totaldata['DC_VOL']/totaldata['TRADE_VOL']
            totaldata['DC_VOL_PERCENT']=totaldata['DC_VOL_PERCENT'].fillna(0.0).round(2)
            #淨成交平均價
            totaldata['NET_GAP_AVE_PRICE']=totaldata['NET_GAP_VALUE']/totaldata['NET_GAP_VOL']
            totaldata['NET_GAP_AVE_PRICE']=totaldata['NET_GAP_AVE_PRICE'].fillna(0.0).round(2)
            #外資成交值 買 跟 賣
            totaldata['QFVOL_BUY_VALUE']=totaldata['QFVOL_BUY']*totaldata['NET_GAP_AVE_PRICE']
            totaldata['QFVOL_SELL_VALUE']=totaldata['QFVOL_SELL']*totaldata['NET_GAP_AVE_PRICE']
            #投信成交值
            totaldata['TRUSTVOL_BUY_VALUE']=totaldata['TRUSTVOL_BUY']*totaldata['NET_GAP_AVE_PRICE']
            totaldata['TRUSTVOL_SELL_VALUE']=totaldata['TRUSTVOL_SELL']*totaldata['NET_GAP_AVE_PRICE']
            #外資累計成交值
            totaldata['QFVOL_BUY_VALUE_CUMSUM']=totaldata['QFVOL_BUY_VALUE'].cumsum()
            totaldata['QFVOL_SELL_VALUE_CUMSUM']=totaldata['QFVOL_SELL_VALUE'].cumsum()
            #投信累計成交值
            totaldata['TRUSTVOL_BUY_VALUE_CUMSUM']=totaldata['TRUSTVOL_BUY_VALUE'].cumsum()
            totaldata['TRUSTVOL_SELL_VALUE_CUMSUM']=totaldata['TRUSTVOL_SELL_VALUE'].cumsum()
            #外資累計成交量
            totaldata['QFVOL_BUY_CUMSUM']=totaldata['QFVOL_BUY'].cumsum()
            totaldata['QFVOL_SELL_CUMSUM']=totaldata['QFVOL_SELL'].cumsum()
            #投信累計成交量
            totaldata['TRUSTVOL_BUY_CUMSUM']=totaldata['TRUSTVOL_BUY'].cumsum()
            totaldata['TRUSTVOL_SELL_CUMSUM']=totaldata['TRUSTVOL_SELL'].cumsum()
            #外資成本價
            totaldata['QF_BUY_COST_PRICE']=totaldata['QFVOL_BUY_VALUE_CUMSUM']/totaldata['QFVOL_BUY_CUMSUM']
            totaldata['QF_BUY_COST_PRICE']=totaldata['QF_BUY_COST_PRICE'].fillna(0.0).round(2)
            totaldata['QF_SELL_COST_PRICE']=totaldata['QFVOL_SELL_VALUE_CUMSUM']/totaldata['QFVOL_SELL_CUMSUM']
            totaldata['QF_SELL_COST_PRICE']=totaldata['QF_SELL_COST_PRICE'].fillna(0.0).round(2)
            #投信成本價
            totaldata['TRUST_BUY_COST_PRICE']=totaldata['TRUSTVOL_BUY_VALUE_CUMSUM']/totaldata['TRUSTVOL_BUY_CUMSUM']
            totaldata['TRUST_BUY_COST_PRICE']=totaldata['TRUST_BUY_COST_PRICE'].fillna(0.0).round(2)
            totaldata['TRUST_SELL_COST_PRICE']=totaldata['TRUSTVOL_SELL_VALUE_CUMSUM']/totaldata['TRUSTVOL_SELL_CUMSUM']
            totaldata['TRUST_SELL_COST_PRICE']=totaldata['TRUST_SELL_COST_PRICE'].fillna(0.0).round(2)
            totaldata['OPEN_PRICE']=totaldata['OPEN_PRICE'].str.replace(',','').astype(float)
            totaldata['CLOSE_PRICE']=totaldata['CLOSE_PRICE'].str.replace(',','').astype(float)
            #量能
            totaldata['AM_ENERGY_VALUE']=(totaldata['OPEN_PRICE']-totaldata['CLOSE_PRICE']).fillna(0.0).abs()/totaldata['NET_GAP_VOL']/0.000001
            totaldata['AM_ENERGY_VALUE']=totaldata['AM_ENERGY_VALUE'].fillna(0.0).round(2)
            #外資平均成本和淨均價 %數
            totaldata['QF_AVG_NET_PRICE_BUY']=totaldata['NET_GAP_AVE_PRICE']/totaldata['QF_BUY_COST_PRICE']
            totaldata['QF_AVG_NET_PRICE_BUY']=totaldata['QF_AVG_NET_PRICE_BUY'].fillna(0.0).round(2)
            totaldata['QF_AVG_NET_PRICE_SELL']=totaldata['NET_GAP_AVE_PRICE']/totaldata['QF_SELL_COST_PRICE']
            totaldata['QF_AVG_NET_PRICE_SELL']=totaldata['QF_AVG_NET_PRICE_SELL'].fillna(0.0).round(2)
            #投信平均成本和淨均價 %數
            totaldata['TRUST_AVG_NET_PRICE_BUY']=totaldata['NET_GAP_AVE_PRICE']/totaldata['TRUST_BUY_COST_PRICE']
            totaldata['TRUST_AVG_NET_PRICE_BUY']=totaldata['TRUST_AVG_NET_PRICE_BUY'].fillna(0.0).round(2)
            totaldata['TRUST_AVG_NET_PRICE_SELL']=totaldata['NET_GAP_AVE_PRICE']/totaldata['TRUST_SELL_COST_PRICE']
            totaldata['TRUST_AVG_NET_PRICE_SELL']=totaldata['TRUST_AVG_NET_PRICE_SELL'].fillna(0.0).round(2)

            # totaldata.to_csv(stockid+".csv",encoding='big5')
            print (totaldata)
            newcolumn=['DATEDAY','STOCKNO','STOCKNAME','TRADE_VOL','TRADE_VALUE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','AVG_A_VALUE','DC_VOL','DC_VAL_PERCENT','NET_GAP_VOL','NET_GAP_VALUE','NET_GAP_AVE_PRICE','QFVOL_BUY','QFVOL_SELL','TRUSTVOL_BUY','TRUSTVOL_SELL','QFVOL_BUY_VALUE','QFVOL_SELL_VALUE','TRUSTVOL_BUY_VALUE','TRUSTVOL_SELL_VALUE','QF_BUY_COST_PRICE','QF_SELL_COST_PRICE','TRUST_BUY_COST_PRICE','TRUST_SELL_COST_PRICE','QF_AVG_NET_PRICE_BUY','QF_AVG_NET_PRICE_SELL','TRUST_AVG_NET_PRICE_BUY','TRUST_AVG_NET_PRICE_SELL','AM_ENERGY_VALUE']
            finaldata=totaldata[newcolumn]

            c2,conn2=Stock_DB_Connection()
            finaldata.to_sql('t'+stockid,con=conn2,if_exists='append',index=False)
            
            conn2.close()
            del totaldata,finaldata,totaldata2

def Init():
    loc_dt = datetime.date.today()
    # SQL_CreateTable()
    # SQL_SB_CreateTable()
    # SQL_DC_CreateTable()
    # 從20190930開始
    max_db_date=check_database_max_date()[0]
    dateGap=loc_dt-datetime.datetime.strptime(max_db_date,'%Y-%m-%d').date()
    # Total_Stock_change_StockTable(max_db_date)
    for i in range(dateGap.days,0,-1):
        time_del = datetime.timedelta(days=i)
        new_dt = loc_dt - time_del
        print(new_dt.isoweekday())
        if new_dt.isoweekday() < 6:  # to fix sql已存在
            new_dt_format = new_dt.strftime("%Y%m%d")
            otc_dt_format = str(new_dt.year-1911)+'/' + \
                new_dt.strftime("%Y/%m/%d")[5:]
            db_date = new_dt.strftime("%Y-%m-%d")
            if twse_check_date_bool(db_date) == True:
                print(new_dt_format)
                t = random.randrange(3000, 7000)  # wait
                time.sleep(t/1000)
                get_stock_histor_twse(new_dt_format, db_date, otc_dt_format)

            if check_buy_sell_bool(db_date) == True:
                print(new_dt_format)
                t = random.randrange(5000, 8000)  # wait
                time.sleep(t/1000)
                Legal_Buy_Sell_infomation(new_dt_format, db_date)
                Tepx_Buy_Sell_information(
                    otc_dt_format, db_date, new_dt_format)
                
            if check_DC_date(db_date) == True:
                print(new_dt_format)
                t = random.randrange(5000, 8000)  # wait
                time.sleep(t/1000)
                Twse_DC_Information(new_dt_format, db_date,otc_dt_format,'True')    #上市
                Twse_DC_Information(new_dt_format, db_date,otc_dt_format,'False')   #上櫃


c, conn = Sqlconnection()
Init()
