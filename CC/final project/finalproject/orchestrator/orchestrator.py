#!/usr/bin/env python
import logging
import pika
import threading
import uuid
import sys
from flask import Flask, Request, jsonify,request ,  Response,json , url_for
from collections import defaultdict
import csv
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm.attributes import flag_modified
#from sqlalchemy.types import *
from flask_marshmallow import Marshmallow
from sqlalchemy import PickleType
#import os
#from crud import *
import datetime
import requests
#import tldextract
import re
import time
import schedule 
import docker
import math
from datetime import datetime
from datetime import date
from multiprocessing import Process
app = Flask(__name__)
#basedir = os.path.abspath(os.path.dirname(__file__))
from multiprocessing import Value
counter = Value("i",0)

connection = pika.BlockingConnection(
	pika.ConnectionParameters(host='rabbitmq',heartbeat=250)) 
channel = connection.channel()


corr_id = str(uuid.uuid4())
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue
response = None
channel.queue_declare(queue='ReadQ',durable=True)  
  
def on_response(ch, method, properties, body):
    global response
    if corr_id == properties.correlation_id:
        response = body
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
   
channel.basic_consume(
            queue=callback_queue,
            on_message_callback=on_response)


from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
logging.basicConfig()
import random
from kazoo.exceptions import NodeExistsError

zk = KazooClient(hosts='zookeeper:2181')
zk.start()
zk.ensure_path("/consumer")
y=random.randint(0,10000)
pat1='/consumer/slave_'+str(y)

def watch_children(event):
    client.containers.run("ubuntu_slave","python slave.py",links={"rabbitmq":"rabbitmq","zookeeper":"zookeeper","master":"master"},volumes_from=['master'],network='ubuntu_default',detach=True)
    

#zk.get_children("/consumer", watch=watch_children)


client = docker.from_env()  
     
flag=0

def foo():    
    num = counter.value/20
    if(num==0):
        num1=1
    else:
        num1 = math.ceil(num)
    no_slaves=0
    no_master=0
    for container in client.containers.list():
        if(container.attrs['Config']['Image']=='ubuntu_slave'):
            no_slaves+=1
        if(container.attrs['Config']['Image']=='ubuntu_master'):
            no_master+=1
    if(no_master==1):
        if(no_slaves<num1):
            diff=num1-no_slaves
            for i in range(diff):
                client.containers.run("ubuntu_slave","python slave.py",links={"rabbitmq":"rabbitmq","master":"master"},volumes_from=['master'],network='ubuntu_default',detach=True)
        else:
            diff1=no_slaves-num1
            count=0
            slaves=[]
            for container in client.containers.list():
                if(container.attrs['Config']['Image']=='ubuntu_slave'):
                    slaves.append(container.id)
            for i in range(diff1):
                container=client.containers.get(slaves[i])
                container.stop(timeout=1)
                container.remove(v=True)
    else:
        num_slaves=no_slaves-1
        if(num_slaves<num1):
            diff=num1-num_slaves
            for i in range(diff):
                client.containers.run("ubuntu_slave","python slave.py",links={"rabbitmq":"rabbitmq","master":"master"},network='ubuntu_default',detach=True)
        else:
            diff1=num_slaves-num1
            count=0
            slaves=[]
            for container in client.containers.list():
                if(container.attrs['Config']['Image']=='ubuntu_slave'):
                    slaves.append(container.id)
            for i in range(diff1):
                container=client.containers.get(slaves[i])
                container.stop(timeout=1)
                container.remove(v=True)            
    with counter.get_lock():
        counter.value = 0
    threading.Timer(120, foo).start()
    




@app.route("/api/v1/db/write", methods=["POST"])
def write_dab():
    connection = pika.BlockingConnection(
	pika.ConnectionParameters(host='rabbitmq')) 
    channel = connection.channel()
    content = ""
    data = request.json
    message = json.dumps(data)
    channel.queue_declare(queue='WriteQ',durable=True)
    channel.basic_publish(exchange='',routing_key='WriteQ',body=message)
    #connection.close()
    return str('write queue')


    
     
@app.route('/api/v1/db/read',methods=['POST'])
def read_dab():
    if counter.value == 0:
        global flag
        if(flag==0):
            foo()
            flag=1
    with counter.get_lock():
        counter.value+=1
    data = request.json
    message = json.dumps(data)
    if data['some'] == "adduser1":
        return jsonify({}),200
    channel.basic_publish(
            exchange='',
            routing_key='ReadQ',
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=corr_id,
            ),
            body=message) 
    while True:
        st = ''
        global response
        if response is None:
            connection.process_data_events()
            continue
        elif response is not None:
            dict_str = response.decode("UTF-8")
            st = dict_str
            break
    response = None
    return str(dict_str)
@app.route('/api/v1/worker/list',methods=['GET'])
def get_pid():
    client1 = docker.APIClient()
    v = client1.inspect_container('rabbitmq')
    li = []
    for container in client.containers.list():
        if(container.attrs['Config']['Image']=='ubuntu_slave'):
            li.append(container.id)
    pid = []
    for i in li:
        v = client1.inspect_container(i)
        l = v['State']['Pid']
        pid.append(int(l))
    pid.sort()
    return str(pid)

@app.route('/api/v1/crash/slave',methods=['POST'])
def slave_crash():
    client1 = docker.APIClient()
    v = client1.inspect_container('rabbitmq')
    li = []  
    pid=[]
    for container in client.containers.list():
        if(container.attrs['Config']['Image']=='ubuntu_slave'):
            li.append(container.id)
    max_pid=0
    max_containerid=0
    for i in li:
        v = client1.inspect_container(i)
        l = v['State']['Pid']
        if(int(l)>max_pid):
            max_pid=int(l)
            max_containerid=i
    container=client.containers.get(max_containerid)
    container.stop(timeout=1)
    container.remove(v=True)
    #container.kill()
    children = zk.get_children("/consumer/", watch=watch_children)
    pid.append(max_pid)
    return str(pid)

@app.route('/api/v1/crash/master',methods=['POST'])
def master_crash():
   
    client1 = docker.APIClient()
    v = client1.inspect_container('rabbitmq')
    li = []  
    pid=[]
    for container in client.containers.list():
        if(container.attrs['Config']['Image']=='ubuntu_slave'):
            li.append(container.id)
    min_pid=100000
    min_containerid=0
    for i in li:
        v = client1.inspect_container(i)
        l = v['State']['Pid']
        if(int(l)<min_pid):
            min_pid=int(l)
            min_containerid=i

    container=client.containers.get(min_containerid)
    container.stop(timeout=1)
    container.remove(v=True)

    pid.append(min_pid)
    return str(pid)


@app.route("/api/v1/db/clear", methods=["POST"])
def clear_dab():
    dicc={}
    dicc["some"]="cleardb"
    message = json.dumps(dicc)
    channel.queue_declare(queue='WriteQ',durable=True)
    channel.basic_publish(exchange='',routing_key='WriteQ',body=message)
    return str('clear db')






if __name__ == '__main__':
    app.run(host="0.0.0.0",port=80,debug=True,use_reloader=False)
        
    

