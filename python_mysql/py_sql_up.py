
# 将数据添加到数据库（数据库已经提前建好）


import pandas as pd
import pymysql
import re

def mycursor(db_name = 'mysql80'):
    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',#123456
                             port = 3308,
                             database= db_name,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    return cursor, connection

def use(db_name):
    '''切换数据库，返回游标'''
    return mycursor(db_name)

def query(sql):
    '''以数据框形式返回查询据结果'''
    cursor.execute(sql)
    data = cursor.fetchall()  # 以元组形式返回查询数据
    header = [t[0] for t in cursor.description]
    df = pd.DataFrame(list(data), columns=header)  # pd.DataFrem 对列表具有更好的兼容性
    return df
    # print(df)

def show_databases():
    '''查看服务器上的所有数据库'''
    sql = 'show databases;'
    return query(sql)

def select_database():
    '''查看当前数据库'''
    sql = 'select database();'
    return query(sql)

def show_tables():
    '''查看当前数据库中所有的表'''
    sql = 'show tables;'
    return query(sql)

cursor, db = use('mysql') #默认设置，不要更改！
dbname = 'kgg'
cursor, db = use(dbname)

#-------------------------------------

# 将ann文件数据传入（暂以txt格式传入）
txtfile = open(r'C:\Users\DELL\Desktop\mysql数据库\0001-0500.ann', 'r', encoding='utf-8').readlines()
# print(txtfile[:3])

#将实体与关系数据分开
entity = []
relation = []
for i in txtfile:
    if i[0] == 'T':
        entity.append(i)
    else:
        relation.append(i)

# print(relation)

#将实体名称、类别与编号提取出来
entity = [i.strip('\n').split('\t') for i in entity]
entity = [[i[0],i[1].split(' ')[0], i[-1]] for i in entity]
# print(entity[:5])

#-------------------------------------------------------------------------
#在实体表中插入数据
# #提取实体名，去重;需要提前查看数据库已有的实体名称 需要表名称
entitymin = [i[-1] for i in entity]
entitymin = list(set(entitymin))
# print(entitymin[:5])


for q in entitymin:
    sqlq = "select name from entitymin where  name = (%s);"
    TF = cursor.execute(sqlq, q)
    if TF == 0: #存在则为1，不存在为0;不存在则添加    
        sqlin2 = "insert into entitymin values (null, %s) ;"
        cursor.execute(sqlin2, q)
db.commit() #事务
# 查看数据效果
sqlq = "select * from entitymin;"
query(sqlq)

#-------------------------------------------------------------------------
#将三元组提取出来
relation = [re.split("[\tA:' ']",i) for i in relation]
relation = [[i[1],i[4],i[7]] for i in relation]
# print(relation[:5])

dicen = dict([('病症',1),('病名',2),('诊断方案',3),('治疗方案',4),('药名',5),('其它',6)])
dicre =  dict([('包含',1),('治疗',2),('危险因素',3),('辅助诊断',4),('特征',5),('并发',6),('别名',7),('作用',8),('条件',9)])
# print(dicre['包含'])

# #将三元组中的实体编号替换成实体名称
for r in relation:
    r[0] = dicre[r[0]]
    for e in entity:
        if r[1] == e[0]:
            r[1] = e[-1]
            r.insert(0,e[1])
        if r[-1] == e[0]:
            r[-1] = e[-1]
            r.append(e[1])
# print(relation[:5])

#编码化---['头实体类', '头实体','关系类','尾实体', '尾实体类']
enre = []
for j in relation:
    j[0] = dicen[j[0]]
    j[-1] = dicen[j[-1]]
    sqlchaen = "select id from entitymin where name = (%s);"
    cursor.execute(sqlchaen, j[2])
    j[2] = cursor.fetchone()['id']
    sqlchaen = "select id from entitymin where name = (%s);"
    cursor.execute(sqlchaen, j[3])
    j[3] = cursor.fetchone()['id']
    enre.append([j[0],j[2],j[1],j[3],j[4]])
print(enre[-5:]) #传入实体-关系库


#-------------------------------------------------------------------------
#插入实体-关系库数据，并创建链接该表（子表、外键）与另外三张表（父表、主键）链接

# #插入数据
for en in enre:
    sqlin2 = "insert into entityrela values (null, %s, %s, %s, %s, %s) ;"
    cursor.execute(sqlin2, (en[0],en[1],en[2],en[3],en[4]))
db.commit() #事务
print('ok')


#-------------------------------------------------------------------------
#清除重复数据并id排序
sql = '''
delete p1
from entityrela p1,
     entityrela p2
where (p1.headclass = p2.headclass
  and p1.headentity = p2.headentity
  and p1.relation = p2.relation
  and p1.tailentity = p2.tailentity
  and p1.tailclass = p2.tailclass
  and p1.id > p2.id);
'''
cursor.execute(sql)

sql = '''
ALTER TABLE entityrela DROP id;
'''
cursor.execute(sql)
sql ='''
ALTER TABLE entityrela ADD id MEDIUMINT( 8 ) NOT NULL FIRST;
'''
cursor.execute(sql)
sql ='''
ALTER TABLE entityrela MODIFY COLUMN id MEDIUMINT( 8 ) NOT NULL AUTO_INCREMENT,ADD PRIMARY KEY(id);
'''
cursor.execute(sql)
db.commit() #事务
cursor.close()
#-------------------------------------------------------------------------
#end
