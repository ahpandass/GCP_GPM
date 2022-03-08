import random

path = 'c:/Lex/dim_product.csv'
f = open(path,'r')
stmt1 = f.read()
f.close()
stmt = ''
for line in stmt1.split('\n'):
    if len(line)>1:
        (product_name, product_id, color, product_type, product_style, department, category, department_id, cost, price)=line.split(',')
        PE_Category = ''
        color_id = str.join('',[str(random.randint(0,9)),str(random.randint(0,9)),str(random.randint(0,9))])
        if product_id[0:2] == 'fw':
            PE_Category = 'FTW'
        if product_id[0:3]=='acc':
            PE_Category = 'EQP'
        if product_id[0:3]=='app':
            PE_Category = 'APP'
        if category=='Training':
            department_id = '1'
        if category=="""Men's""":
            department_id = '2'
        if category=="""Women's""":
            department_id = '3'
        stmt += str.join(',',[product_name, product_id, color_id,PE_Category, product_type, product_style, department, category, department_id, price ])+'\n'

path = 'C:/Lex/GCP/GCP_GPM/product.csv'    
f = open(path,'w')
f.write(stmt)
f.close()