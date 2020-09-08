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
from datetime import datetime
from datetime import date
app = Flask(__name__)
#basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///CC_0024_0162_1618_1640_app.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
dab = SQLAlchemy(app)
ma = Marshmallow(app)
from multiprocessing import Value
counter = Value("i",0)



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


user_schema = UserSchema()

dab.create_all()

@app.route("/api/v1/users" ,methods=["POST","DELETE"])
def add_user1():
    with counter.get_lock():
        counter.value+=1
    dictt={}
    dictt["some"] = "adduser1"
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, json = dictt, headers=headers)
    return jsonify({}), 405


@app.route("/api/v1/users" ,methods=["PUT"])
def add_user():
    with counter.get_lock():
        counter.value+=1
    dic = {}
    dictt={}
    dictt["some"] = "adduser"
    dictt['username'] = request.json['username']
    url="http://34.204.31.34:80/api/v1/db/read"
    #data = {'username':"gagana","password":"dskjf85u6i"}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, json = dictt, headers=headers)
    #return r.text
    password = request.json['password']
    pattren = re.compile('^[0-9a-f]{40}$')
    matches = pattren.match(password)
    if r.text != "None":
        return jsonify({'a':'a'}),400
    if not re.match(pattren,password):
        return jsonify({'b':'b'}),400
    else:
        dic["username"] = request.json['username']
        dic["password"] = request.json['password']
        username = request.json['username']
        dic["some"] = "user"
        url="http://34.204.31.34:80/api/v1/db/write"
        #data = {'username':"gagana","password":"dskjf85u6i"}
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url, json = dic, headers=headers)
        return jsonify({}),201

@app.route("/api/v1/users/<username>", methods=["PUT","POST","GET"])
def user_delete1():
    with counter.get_lock():
        counter.value+=1
    dictt["some"] = "adduser1"
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, json = dictt, headers=headers)
    return jsonify({}),405

@app.route("/api/v1/users/<username>", methods=["DELETE"])
def user_delete(username):
    with counter.get_lock():
        counter.value+=1
    dictt={}
    dictt["some"] = "deluser"
    dictt['username'] = username
    url="http://34.204.31.34:80/api/v1/db/read"
    request.headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, json = dictt, headers=request.headers)
    #print(r.text)
    #print(r)
    if r.text=="None":
        return jsonify({}),400
    else:
        url="http://34.204.31.34:80/api/v1/db/write"
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        res = requests.post(url, json = dictt, headers=headers)
        return jsonify({}),200


@app.route("/api/v1/users",methods=["GET"] )
def get_users():
    with counter.get_lock():
        counter.value+=1
    dic={}
    dic['some']="get_user"
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url,json = dic ,headers = headers)
    if (r.text=="[]"):
        return jsonify({}),204
    else:
        return r.text


@app.route("/api/v1/_count",methods=['GET'])
def get_count():
    li = []
    li.append(counter.value)
    return str(li)

@app.route("/api/v1/_count",methods=['DELETE'])
def reset_count():
    with counter.get_lock():
        counter.value = 0
    return jsonify({}),200            
            
if __name__ == '__main__':
    app.run(host="0.0.0.0",port=80,debug=True)

