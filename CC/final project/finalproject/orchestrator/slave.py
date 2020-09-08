#!/usr/bin/env python
import random
y=random.randint(0,100000)
s="slave_"
name=s+str(y)
import logging
import sqlite3
from kazoo.client import KazooClient
from kazoo.client import KazooState

logging.basicConfig()
zk = KazooClient(hosts='zookeeper:2181')
zk.start()
zk.ensure_path("/consumer")
pat="/consumer/slave_"+str(y)


if zk.exists(path=pat):
    pass
else:
    zk.create(path=pat, value=b'gaani',ephemeral=True)

from multiprocessing import Value
#counter2 = Value("i",0)
counter = Value("i",0)
import pika
import time
import ast
import threading
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

x='sqlite:///db'
z='.sqlite'

database = x+str(y)+z


#########copy from db to this database#############

app = Flask(__name__)
#basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = database

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
dab = SQLAlchemy(app)

dt='db'+str(y)+z



ma = Marshmallow(app)


old_db=sqlite3.connect('db.sqlite')



curr1=old_db.cursor()
curr1.execute("SELECT * FROM User")


rows = curr1.fetchall()







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


for row in rows:
    old_user = User(row[0], row[1])
    dab.session.add(old_user)
    dab.session.commit()
    


curr1.execute("SELECT * FROM Ride")


rows = curr1.fetchall()

 
for row in rows:
    old_ride = Ride(row[1], row[2],row[3],row[4],row[5])
    dab.session.add(old_ride)
    dab.session.commit()
  





connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq',heartbeat=250))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

channel.queue_declare(queue='ReadQ', durable=True)
#channel.queue_declare(queue='SynQ', durable=True)
result = channel.queue_declare(queue='', durable=True)
queue_name = result.method.queue

channel.queue_bind(exchange='logs', queue=queue_name)

def converter(string):
    arr=string.split(',')
    arr1=arr[0].split('/')
    arr2=arr[1].split(':')
    date=arr1[1]+"-"+arr1[0]+"-"+arr1[2]+":"+arr2[2]+"-"+arr2[1]+"-"+(arr2[0]).strip(" ") 
    return str(date)


#print(' [*] Waiting for messages.')


def callback1(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    dict_str = body.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    if(mydata['some'] == "user"):
        content = mydata
        username = content["username"]
        password = content["password"]
        new_user = User(username, password)
        dab.session.add(new_user)
        dab.session.commit()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #connection.close()
        #print('user craeted by syncing ')
        #return str('addedd user')
        #print('wowww gaani')
        #return str('write queue')
    elif (mydata['some'] == 'deluser'):
        content = mydata
        username = content['username']
        user=User.query.get(username)
        dab.session.delete(user)
        dab.session.commit()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #return str(user_schema.jsonify(user))
        #print('user gets deleted by syncing')
        #return str('deluser')
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
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #print('create ride done by syncing')
        #return str('create rides')
    elif (mydata['some'] == 'delete_ride'):
        content = mydata
        rideId = content['rideId']
        ride=Ride.query.get(rideId)
        dab.session.delete(ride)
        dab.session.commit()
        #return ride_schema.jsonify(ride)
        #channel.basic_publish(exchange='',routing_key='SyncQ',body=body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #print('delete ride done by syncing')
        #return str('delete ride')

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
            #channel.basic_publish(exchange='',routing_key='SyncQ',body=body)
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
        ch.basic_ack(delivery_tag=method.delivery_tag)


   
def callback(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    dict_str = body.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    #print(repr(mydata))
    #print(type(mydata))
    channel.basic_qos(prefetch_count=1)
    #print('get_user')
    if(mydata['some'] == 'get_user'):
        lis=[]
        users=User.query.all()
        for user in users:
            lis.append(user.username)
        #print('get_user1')
        list1=str(lis)
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=list1)

        #print('get_user2')       
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #print('get_user')
        #print(str(lis))
    elif mydata['some'] == 'adduser':
        content = mydata
        username = content['username']
        user=User.query.get(username)
        #print(user)
        #print(username)
        #return user_schema.jsonify(user)
        #print('adduser1')
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=str(user))   
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    elif(mydata['some'] == 'deluser'):
        content = mydata
        username = content['username']
        user=User.query.get(username)
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=str(user))      
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #return str('del user')
    elif (mydata['some'] == 'ridesid'):
        content = mydata
        dictt={}
        rideId = content['rideId']
        ride=Ride.query.get(rideId)
        #return 'okay'
        if ride is None:
            ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=str(ride))
            ch.basic_ack(delivery_tag=method.delivery_tag)
      
        else:
            riders=ride.riders
            ID = ride.rideId
            dictt["created_by"] = ride.created_by
            dictt['timestamp'] = ride.timestamp
            dictt["rideId"]=ride.rideId
            dictt["riders"]=ride.riders
            dictt['source'] = ride.source
            dictt['destination'] = ride.destination
            y=json.dumps(dictt)
            #r = ride_schema.jsonify(ride)
            ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=y)
            ch.basic_ack(delivery_tag=method.delivery_tag)
       

    elif (mydata['some'] == "join_ride"):
        content = mydata
        username=content['username']
        rideId=content['rideId']
        exists=dab.session.query(Ride.rideId).filter_by(rideId=rideId).scalar()
        dic={}
        dic['exists'] = exists
        message = json.dumps(dic)
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    elif(mydata['some']=='delete_ride'):
        content = mydata
        rideId = content['rideId']
        exists=dab.session.query(Ride.rideId).filter_by(rideId=rideId).scalar()
        dic = {}
        dic['exists'] = exists
        r  = dic
        message = json.dumps(dic)
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    elif (mydata['some'] == 'get_details'):
        content=mydata
        riders=content['riders']
        new_riders=[]
        lis=[]
        users=User.query.all()
        for user in users:
            lis.append(user.username)
        for rider in riders:
            if rider in lis:
                new_riders.append(rider)
            else:
                pass
        #r = jsonify(new_riders)
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=str(new_riders))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #return jsonify(new_riders)

    elif(mydata['some'] == 'list'):     
        content1 = mydata
        source = content1['source']
        destination = content1['destination']    
        ride_id=dab.session.query(Ride.rideId).filter(Ride.source==source,Ride.destination==destination).all()
        s=str(ride_id)
        lis=[]
        x=map(int,re.findall(r'\d+',s))
        for i in list(x):
            ride=Ride.query.get(i)
            dumb={}
            dumb["rideId"]=ride.rideId
            dumb["username"]=ride.created_by
            username = ride.created_by
            timestamp = ride.timestamp
            now = datetime.datetime.now()
            timestamp = datetime.datetime.strptime(timestamp, '%m/%d/%Y, %H:%M:%S')
            users = User.query.all()
            lis1 = []
            for i in users:
                lis1.append(i.username)
            if username not in lis1:
                pass    
            elif now > timestamp:
                pass
            else:
                dumb["timestamp"]=converter(ride.timestamp)
                lis.append(dumb)
        if(len(lis)==0):
            r = 'None'
            ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=r)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        else:
            #return jsonify(lis)
            #message = json.dumps(lis)
            ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=str(lis))
            ch.basic_ack(delivery_tag=method.delivery_tag)

       
    elif (mydata['some'] == 'ride_count'):
        rides=Ride.query.all()
        for ride in rides:
            count+=1
        li = []
        li.append(count)
        ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=str(li))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
        

    

                                        
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='ReadQ', on_message_callback=callback)
#channel.basic_consume(queue='SyncQ', on_message_callback=callback1)

channel.basic_consume(
    queue=queue_name, on_message_callback=callback1)


try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()



if __name__ == '__main__':
    app.run(host="0.0.0.0",port=80,debug=True) 



