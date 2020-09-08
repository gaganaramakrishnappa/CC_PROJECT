#!/usr/bin/env python
import pika
import time
import ast
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
'''from db import User
from db import Ride
from db import RideSchema
from db import UserSchema
from db import dab
ride_schema=RideSchema()
rides_schema=RideSchema(many=True)
user_schema = UserSchema()'''


app = Flask(__name__)
#basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
dab = SQLAlchemy(app)
ma = Marshmallow(app)
from multiprocessing import Value
counter = Value("i",0)

import docker
class User(dab.Model):
    #id = dab.Column(dab.Integer, primary_key=True)
    username = dab.Column(dab.String(),primary_key=True)
    password = dab.Column(dab.String())

    def __init__(self, username, password):
        self.username = username
        self.password = password

class UserSchema(ma.Schema):
    class Meta:
        # Fields to expose
        fields = ('username', 'password')

class Ride(dab.Model):
    rideId=dab.Column(dab.Integer(),primary_key=True)
    created_by=dab.Column(dab.String())
    source=dab.Column(dab.Integer())
    destination=dab.Column(dab.Integer())
    timestamp=dab.Column(dab.String())
    riders=dab.Column(dab.PickleType())
    #users = dab.Column(dab.String())

    def __init__(self,created_by,source,destination,timestamp,riders=[]):
        self.created_by=created_by
        self.source=source
        self.destination=destination
        self.timestamp=timestamp
        self.riders=riders
        #self.users=users

class RideSchema(ma.Schema):
    class Meta:
        fields=('created_by','source','destination','timestamp','riders')

ride_schema=RideSchema()
rides_schema=RideSchema(many=True)
user_schema = UserSchema()
dab.create_all()


import logging
client=docker.APIClient()
client2=docker.from_env()

li=[]
for container in client2.containers.list():
    li.append(container.id)

pid=[]
image=[]

for i in li:
    v=client.inspect_container(i)
    l=v['State']['Pid']
    i=v['Config']['Image']
    pid.append((l,i))

v=client.inspect_container('master')
pid_of_master=v['State']['Pid']
res = bytes(str(pid_of_master), 'utf-8') 
from kazoo.client import KazooClient
from kazoo.client import KazooState

logging.basicConfig()


zk = KazooClient(hosts='zookeeper:2181')
zk.start()

zk.ensure_path("/producer")
pat = "/producer/master"
if zk.exists("/producer/master"):
    #print("master already exists")
    pass
else:
    zk.create(path=pat,value=res,ephemeral=True)
    #print("created master_1")

data, stat = zk.get("/producer/master")
#print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq',heartbeat=250))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

channel.queue_declare(queue='WriteQ',durable=True)
#channel.queue_declare(queue='SyncQ', durable=True)


#print(' [*] Waiting for messages.')

def callback(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    dict_str = body.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    #print(repr(mydata))
    #print(type(mydata))
    #channel.basic_qos(prefetch_count=1)
    #print('get_user')
    if (mydata['some'] == "user"):
        content = mydata
        username = content["username"]
        password = content["password"]
        new_user = User(username, password)
        dab.session.add(new_user)
        dab.session.commit()
        #print('commited')
        channel.basic_publish(exchange='logs',routing_key='',body=body)
        #print('after commition')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return str(content)
    
    elif (mydata['some'] == 'deluser'):
        content = mydata
        username = content['username']
        user=User.query.get(username)
        #print(user)
        #print(username)
        dab.session.delete(user)
        dab.session.commit()
        #print(user_schema.jsonify(user))
        #print('deluser1')
        #c = user_schema.jsonify(user)
        #rint(type(c))
        #return str('deluser')
        channel.basic_publish(exchange='logs',routing_key='',body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #return user_schema.jsonify(user)
        return str('deluser')
    elif(mydata['some'] == "rides"):
        content = mydata
        created_by = content['created_by']
        source = content['source']
        destination = content['destination']
        timestamp = content['timestamp']
        riders = content['riders']
        #riders.append(created_by)
        new_ride = Ride(created_by,source,destination,timestamp,riders )
        dab.session.add(new_ride)
        dab.session.commit()
        channel.basic_publish(exchange='logs',routing_key='',body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return str('create rides')
    elif (mydata['some'] == 'delete_ride'):
        content = mydata
        rideId = content['rideId']
        ride=Ride.query.get(rideId)
        dab.session.delete(ride)
        dab.session.commit()
        #return ride_schema.jsonify(ride)
        channel.basic_publish(exchange='logs',routing_key='',body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return str('delete ride')

    elif (mydata['some'] == 'join_rides'):
        content = mydata
        rideId = content['rideId']
        username = content['username']
        ride=Ride.query.filter_by(rideId=rideId).first()
        x=ride.riders
        if (username not in x):
            x.append(username)
            ride.riders=x
            flag_modified(ride,"riders")
            dab.session.merge(ride)
            dab.session.flush()
            dab.session.commit()
            #return ride_schema.jsonify(ride)
            channel.basic_publish(exchange='logs',routing_key='',body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
    elif(mydata['some'] == 'cleardb'):
        users=User.query.all()
        for user in users:
            dab.session.delete(user)
            dab.session.commit()
        rides=Ride.query.all()
        for ride in rides:
            dab.session.delete(ride)
            dab.session.commit()
        channel.basic_publish(exchange='logs',routing_key='',body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


    

     
    
channel.basic_consume(queue='WriteQ', on_message_callback=callback)
channel.basic_qos(prefetch_count=1)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()




if __name__ == '__main__':
    app.run(host="0.0.0.0",port=80,debug=True) 
