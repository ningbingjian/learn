# -*- coding: utf-8 -*-
'''

Flask 简单功能测试  登陆验证

'''


from flask import *
app = Flask(__name__)
#app.route是个装饰器
@app.route('/',methods=['GET','POST'])
def home():
    return '<h1>home</h1>';
@app.route('/sign_form',methods=['GET','POST'])
def sign_form():
    return """
    <form action='/sign_in' method='post'>
        <input name='username' type='text' /> <br>
        <input name='password' type='password' /> <br>
        <input type='submit' value='登陆'/> <br>
    </form>

    """
#
@app.route('/sign_in',methods=['GET','POST'])
def sign_in():
    if request.form['password'] == 'admin':
        return """
        <h1>hello ,%s</h1>
        """ % request.form['username']
    else :
        return """
        <h1>username or password not right!!!</h1>
        """


if __name__ == '__main__':
    app.run()
