# -*- coding: utf-8 -*-
#Web Server Gateway Interface。 web服务网关接口
#开发者必须实现application函数 wsgi负责调用
#标准的一个HTTP处理函数

def application1(environ, start_response):
    '''
        environ 包含http的所有请求信息
        start_response 发送http响应信息的函数
        处理流程:
        1、evn 得到请求信息
        2、start_response 响应头部
        3、return 返回body内容
    '''
    start_response('200 ok',[('Content-Type','text/html')])
    return "<h1>hello web</h1>"

def application2(environ, start_response):
    '''
        environ 包含http的所有请求信息
        start_response 发送http响应信息的函数
        处理流程:
        1、evn 得到请求信息
        2、start_response 响应头部
        3、return 返回body内容
    '''
    print environ
    start_response('200 ok',[('Content-Type','text/html')])
    return "<h1>hello %s</h1>" % (environ['PATH_INFO'][1:] or 'web')

#wsgiref 是wsgi标准的"粗糙"实现

from wsgiref.simple_server import make_server
httpd = make_server('',8888,application2)
print 'http start on 8888'
#http://localhost:8888/  访问即可看到效果
httpd.serve_forever()

