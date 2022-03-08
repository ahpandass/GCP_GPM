path = 'C:/Lex/GCP/GCP_GPM/product.csv'
from datetime import datetime,timedelta
import random
def random_index(rate):
    start = 0
    index = 0
    randnum = random.randint(1, sum(rate))
    for index, scope in enumerate(rate):
        start += scope
        if randnum <= start:
            break
    return index

def order_item(order_id,start_date):
    no_of_line = random_index([60,20,15,5])+1
    orderitem_no_pre = start_date.strftime('%Y%m%d%H%M%S')
    order_item_stmt = ''
    order_amt = 0
    for i in range(no_of_line):
        goodlist_id = random.randint(0,len(prodlist))-1
        order_id = order_id
        orderitem_id = str(i)
        line_number = str(orderitem_no_pre)+str(i)
        good_id = prodlist[goodlist_id][1]
        good_name = prodlist[goodlist_id][0]
        qty = str(random_index([80,15,5])+1)
        price = str(prodlist[goodlist_id][2])
        sale_price = price
        msrp = price
        amount = round(float(qty)*float(sale_price),2)
        order_amt += amount
        order_item_stmt += str.join(',',[order_id,orderitem_id,line_number,good_id,good_name,qty,price,sale_price,msrp,str(amount)])+'\n'
    return order_item_stmt,str(order_amt)
    


f = open(path,'r')
stmt1 = f.read()
f.close()
prodlist = []
strt = 0
for line in stmt1.split('\n'):
    if strt == 1 and len(line)>1:
        prod = line.split(',')
        prodlist.append([prod[0], prod[1], float(prod[-1][1:])])
    else:
        strt = 1
        

start_date = datetime.strptime('2020-05-10 14:00:00',"%Y-%m-%d %H:%M:%S")
order_start_id = 110043401
stmt = 'order_id,shop_id,ship_to_cust_id,ship_from_shop_id,sales_amt,delivery_fee,saler_id,coupon_id,discount_amt,post_date,post_time,customer_id,self_pick_flg,acct_1,acct_2,acct_3,description_1,description_2,description_3\n'
orderitem_stmt = 'order_id,orderitem_id,line_number,good_id,good_name,qty,price,sale_price,msrp,amount\n'
for i in range(50000):
    order_id = str(order_start_id+i)
    shop_id_seed = random.randint(0,5)
    if shop_id_seed == 0:
        shop_id = '352'
    if shop_id_seed == 1:
        shop_id = '479'
    if shop_id_seed == 2:
        shop_id = '155'
    if shop_id_seed == 3:
        shop_id = '626'
    if shop_id_seed == 4:
        shop_id = '188'
    if shop_id_seed == 5:
        shop_id = '323'
    ship_to_cust_id = str(random.randint(2130500,2230500))
    ship_from_shop_id = shop_id
    delivery_fee   = '' 
    saler_id       = ''
    coupon_id      = ''
    discount_amt   = ''    
    deltasec = random.randint(0,600) 
    start_date = start_date+timedelta(seconds=deltasec)
    (orderitem_stmt_line, sales_amt) = order_item(order_id, start_date)
    post_date      = start_date.strftime('%Y-%m-%d')
    post_time      = start_date.strftime('%H:%M:%S') 
    customer_id    = ship_to_cust_id 
    self_pick_flg  = '0' 
    acct_1         = 'CNY'
    acct_2         = '' 
    acct_3         = ''
    description_1  = '' 
    description_2  = ''
    description_3  = ''
    stmt += str.join(',',[order_id,shop_id,ship_to_cust_id,ship_from_shop_id,str(sales_amt),delivery_fee,saler_id,coupon_id,discount_amt,post_date,post_time,customer_id,self_pick_flg,acct_1,acct_2,acct_3,description_1,description_2,description_3])+'\n'
    orderitem_stmt += orderitem_stmt_line

path = 'C:/Lex/GCP/GCP_GPM/order.csv'    
f = open(path,'w')
f.write(stmt)
f.close()

path = 'C:/Lex/GCP/GCP_GPM/orderitem.csv'    
f = open(path,'w')
f.write(orderitem_stmt)
f.close()
    
