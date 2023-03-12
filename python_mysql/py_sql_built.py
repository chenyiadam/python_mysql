# @author：陈燚
# time：2022-3-17
# 数据库建设

import pandas as pd
import pymysql

print("1")

def mycursor(db_name = 'mysql80'):
    connection = pymysql.connect(host='localhost',
                             user='root',
                             port = 3308,
                             password='',#123456
                             database= db_name,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    return cursor, connection

def use(db_name):
    '''切换数据库，返回游标'''
    return mycursor(db_name)

def create_database(db_name):
    '''新建数据库'''
    sql = f'create database if not exists {db_name};'
    cursor.execute(sql)
    
def create_table(tbl_name):
    '''新建数据表'''
    sql = f'create table if not exists {tbl_name};'
    cursor.execute(sql) 
    
def drop_database(db_name):
    '''删除数据库'''
    sql = f'drop database if exists {db_name};'
    cursor.execute(sql)
       
def drop_table(tbl_name):
    '''删除数据表'''
    sql = f'drop table if exists {tbl_name};'
    cursor.execute(sql)

def query(sql):
    '''以数据框形式返回查询据结果'''
    cursor.execute(sql)
    data = cursor.fetchall()  # 以元组形式返回查询数据
    header = [t[0] for t in cursor.description]
    df = pd.DataFrame(list(data), columns=header)  # pd.DataFrem 对列表具有更好的兼容性
    # return df
    print(df)

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

#-------------------------------------------------------------------------

#选择需要使用的数据库，此前需要创建数据库


cursor, db = use('mysql') #默认设置，不要更改！

dbname = 'kgkgkg' #此处更改为需要创建的数据库名
create_database(dbname)
cursor, db = use(dbname)
 
#-------------------------------------------------------------------------
# # #格式化数据库
# drop_database(dbname)
# create_database(dbname)
# print('数据库格式化')
#-------------------------------------------------------------------------

# 建实体类表
sqltb1 = ''' create table entity(
    id int auto_increment primary key comment '实体类编号',
    name varchar(20) comment '实体类名'
) comment '实体类表';
'''
cursor.execute(sqltb1)

# 插入数据
sqlin1  = '''
 insert into entity values (1, '病症'),
 (2, '病名'),(3, '诊断方案'),(4, '治疗方案'),(5, '药名'),(6, '其它');
'''
cursor.execute(sqlin1)
db.commit() #事务
# 检查是否传递成功
sqlset = "select * from entity ;"
query(sqlset)

#-------------------------------------------------------------------------

# 创建关系表格
sqltb2 = ''' create table relation(
    id int auto_increment primary key comment '关系编号',
    name varchar(20) comment '关系名'
) comment '关系表';
'''
cursor.execute(sqltb2)

#插入数据
sqlin2 = '''insert into relation values (1, '包含'),(2, '治疗'),(3, '危险因素'),
                                    (4, '辅助诊断'),(5, '特征'),(6, '并发'),
                                    (7, '别名'),(8, '作用'),(9, '条件');'''

cursor.execute(sqlin2)
db.commit() #事务
#检查是否传递成功
sqlset = "select * from relation ;"
query(sqlset)

#-------------------------------------------------------------------------

# 创建实体表
sqltb3 = ''' create table entitymin(
    id int auto_increment primary key comment '编号',
    name varchar(50) comment '实体名'
) comment '实体表';
'''
cursor.execute(sqltb3)
db.commit() #事务

#-------------------------------------------------------------------------

#建立 实体-关系库，并设置主键外键关联

# 创建实体表
sqltb5 = ''' create table entityrela(
    id int auto_increment primary key comment '编号',
    headclass int comment '头实体类',
    headentity int comment '头实体',
    relation int comment '关系',
    tailentity int comment '尾实体',
    tailclass int comment '尾实体类'
) comment '实体和关系表';
'''
cursor.execute(sqltb5)
db.commit() #事务

#-------------------------------------------------------------------------
#创建链接该表（子表、外键）与另外三张表（父表、主键）链接

sqlkey = ''' 
alter table entityrela add constraint encl_he_en_id foreign key (headclass) references entity (id);
'''
cursor.execute(sqlkey)

sqlkey = ''' 
alter table entityrela add constraint enla_he_enmin_id foreign key (headentity) references entitymin (id);
'''
cursor.execute(sqlkey)

sqlkey = ''' 
alter table entityrela add constraint enla_re_re_id foreign key (relation) references relation (id);
'''
cursor.execute(sqlkey)

sqlkey = ''' 
alter table entityrela add constraint enla_ta_enmin_id foreign key (tailentity) references entitymin (id);
'''
cursor.execute(sqlkey)

sqlkey = ''' 
alter table entityrela add constraint enla_ta_en_id foreign key (tailclass) references entity (id);
'''
cursor.execute(sqlkey)
db.commit() #事务

#-------------------------------------------------------------------------

# 查看该库中所有表格、查看表是否创建成功
show_tables()

#-------------------------------------------------------------------------
# end
