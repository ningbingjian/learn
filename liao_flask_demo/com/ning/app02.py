# -*- coding: utf-8 -*-
'''
templates模板必须和当前.py文件在同级目录下


'''
from flask import *
app = Flask(__name__)
@app.route('/',methods=['GET','POST'])
def home():
    #渲染模板html
    return render_template('home.html')
@app.route('/sign_form',methods=['GET','POST'])
def sign_form():
    return render_template('sign_form.html')
@app.route('/sign_in',methods=['GET','POST'])
def sign_in():
    username = request.form['username']
    password = request.form['password']
    if password == 'admin':
        #render_template 参数1是模板名称 参数2是dict，表示传到页面的属性
        return render_template('sign_in.html',username = username)
    else:
        return render_template('sign_form.html',message = 'bad password !!!',username = username)

@app.route('/show_list',methods=['GET','POST'])
def show_list():
    #显示list
    return render_template('show_list.html',list = [x for x in range(1,10)])

if __name__ == '__main__':
    app.run()