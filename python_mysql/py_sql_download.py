
# 从数据库中调取数据，以中文显示；

import pandas as pd
import pymysql

def mycursor(db_name = 'mysql80'):
    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='', #123456
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
dbname = 'mysql'
cursor, db = use(dbname)
dbname = 'kgg'
cursor, db = use(dbname)

# print(show_tables())

    #a.headentity ,
sql = '''select  m.name from entityrela a left outer join entitymin m on a.headentity = m.id'''
i_s = list(query(sql)['name'])

sql = '''select  m.name from entityrela a left outer join entitymin m on a.tailentity = m.id'''
j_s = list(query(sql)['name'])

sql = '''select  m.name from entityrela a left outer join relation m on a.relation = m.id'''
k_s = list(query(sql)['name'])


eql = '''select  m.name from entityrela a left outer join entity m on a.headclass = m.id'''
ek_1 = list(query(eql)['name'])

eq2 = '''select  m.name from entityrela a left outer join entity m on a.tailclass = m.id'''
ek_2 = list(query(eq2)['name'])



result = [[i,j,k,w,p] for i,j,k,w,p in zip(i_s,ek_1,k_s,ek_2,j_s)]
print(result[:5])
cursor.close()

f = open(r"C:\Users\DELL\Desktop\neo4j_python\data\4.csv",'w', encoding='utf-8')
for i in result:
    f.write('\''+i[0]+'\',\''+i[1]+'\',\''+i[2]+'\',\''+i[3]+'\',\''+i[4]+'\'\n')
f.close()
print('ok')






