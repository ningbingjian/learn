# -*- coding: utf-8 -*-
from peewee import *
from datetime import date
db = MySQLDatabase("test",user="root",password="123456")
class BaseModel(Model):
    class Meta:
        database = db
class Person(BaseModel):
    name = CharField()
    birthday = DateField()
    is_relative = BooleanField()
    def __str__(self):
        return "name = %s,birthday = %s ,is_relative = %s" %(self.name,self.birthday,self.is_relative)

class Pet(BaseModel):
    owner = ForeignKeyField(Person, related_name='pets')
    name = CharField()
    animal_type = CharField()

#连接数据库
db.connect()
'''
#创建表 执行一次即可
#db.create_tables([Person, Pet])
#构建person数据
uncle_bob = Person(name='Bob', birthday=date(1960, 1, 15), is_relative=True)
#保存到数据库 c是受影响的行数
c = uncle_bob.save()

#也可以用.create构建数据模型并且保存【构造和保存同时进行】
grandma = Person.create(name='Grandma', birthday=date(1935, 3, 1), is_relative=True)
herb = Person.create(name='Herb', birthday=date(1950, 5, 5), is_relative=False)
#保存后可以继续修改 然后再保存
grandma.name = 'Grandma L.'
grandma.save()
#设置每个用户的pets
bob_kitty = Pet.create(owner=uncle_bob, name='Kitty', animal_type='cat')
herb_fido = Pet.create(owner=herb, name='Fido', animal_type='dog')
herb_mittens = Pet.create(owner=herb, name='Mittens', animal_type='cat')
herb_mittens_jr = Pet.create(owner=herb, name='Mittens Jr', animal_type='cat')
#删除 返回受影响的行数
herb_mittens.delete_instance()
#修改属性
herb_fido.owner = uncle_bob
herb_fido.save()
#变量名  提醒fido已经是bob的宠物了
bob_fido = herb_fido


#查询一行
grandma = Person.select().where(Person.name == "Grandma L.").get()
print grandma


#查询一行的缩写
grandma = Person.get(Person.name == 'Grandma L.')
print grandma

#查询list
for person in Person.select():
    print person.name,person.birthday


#查询 所有cat类型的宠物  因为原始查询没有包含owner,所以这种查询会有n+1的情况
query = Pet.select().where(Pet.animal_type == 'cat')
for pet in query:
    print pet.name,pet.animal_type,pet.owner.name


#通过join避免n+1查询
query = Pet.select(Pet,Person).join(Person).where(Pet.animal_type == 'cat')
for pet in query:
    print pet.name,pet.owner.name



#查询所有拥有着是Bob的宠物 [Person.name 不可以写成Pet.owner.name]
query = Pet.select().join(Person).where(Person.name == 'Bob')
for pet in query:
    print pet.name,pet.owner.name



##如果已经有一个表示owner对象 可以直接用来做join的in
for pet in Pet.select().where(Pet.owner == uncle_bob):
     print pet.name
#order by
for pet in Pet.select().where(Pet.owner == uncle_bob).order_by(Pet.name):
    print pet.name


#降序
for person in Person.select().order_by(Person.birthday.desc()):
    print person.name,person.birthday


#查询用户和用户对应的宠物  这里又陷入了n+1的问题  往下继续解决
for person in Person.select():
    print person.name,person.pets.count(),'pets'
    for pet in person.pets:
        print ' ',pet.name,pet.animal_type

#解决n+1问题，只会发出一次SQL 说实在 python也不是太好看啊   IDE里根本没法提示这些写法。
subquery = Pet.select(fn.COUNT(Pet.id)).where(Pet.owner == Person.id)
query = Person.select(Person,Pet,subquery.alias('pet_count')).join(Pet,JOIN_LEFT_OUTER).order_by(Person.name)
for person in query.aggregate_rows():
    print person.name,person.pet_count, 'pets'
    for pet in person.pets:
        print '  ' ,pet.name,pet.animal_type




d1940 = date(1940, 1, 1)
d1960 = date(1960, 1, 1)
#查询1940以前或者1960以后的用户  (Person.birthday < d1940) 注意括号
query =  Person.select().where((Person.birthday < d1940) | (Person.birthday > d1960))
#查询1940以后并且1960以前的用户  (Person.birthday < d1940) 注意括号
query =  Person.select().where((Person.birthday > d1940) & (Person.birthday < d1960))
for person in query :
    print person.name,person.birthday

#根据数据库生成模型
python.exe -m pwiz  -e mysql -H localhost -p 3306 -u root -P 123456  test  >test_model.py

'''
#操作完成 记得关闭数据库连接
db.close()





