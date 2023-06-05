import requests

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
