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
'''

#获取一行
grandma = Person.select().where(Person.name == "Grandma L.").get()
print grandma




