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
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///CC_0024_0162_1618_1640_app1.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
dab = SQLAlchemy(app)
ma = Marshmallow(app)
from multiprocessing import Value
counter = Value("i",0)




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

dab.create_all()

def check(number1,number2):
    lis=[]
    for i in range(1,199):
        lis.append(i)
    if int(number1) in lis and  int(number2) in lis:
        return True
    else:
        return False

def converter(string):
    arr=string.split(',')
    arr1=arr[0].split('/')
    arr2=arr[1].split(':')
    date=arr1[1]+"-"+arr1[0]+"-"+arr1[2]+":"+arr2[2]+"-"+arr2[1]+"-"+(arr2[0]).strip(" ") 
    return str(date)

@app.route('/api/v1/rides', methods=['DELETE','PUT'])
def add_ride1():
    with counter.get_lock():
        counter.value += 1
    dictt={}
    dictt["some"] = "adduser1"
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, json = dictt, headers=headers)
    return jsonify({}), 405



@app.route("/api/v1/rides", methods=["POST"])
def new_ride():
    with counter.get_lock():
        counter.value+=1
    dic = {}
    dictt={}
    dictt["some"] = "adduser"
    url="http://Load-balancer-142669967.us-east-1.elb.amazonaws.com:80/api/v1/users"
    #url="http://127.0.0.1:8080/api/v1/users"
    s = request.get_json()["created_by"]
    dictt['username'] = s
    r = requests.get(url,headers = {'Origin':'3.213.139.88'})
    created_by=request.get_json()["created_by"]
    r1=r.text
    x=r1.strip("[")
    y=x.strip("]")
    lis = y.split(",")
    lis1=[]
    for i in lis:
        x=i.strip()
        y=x.strip("'")
        lis1.append(y)
    if created_by in lis1:
        num1=request.get_json()["source"]
        num2=request.get_json()["destination"]
        if num1==num2:         
            return jsonify({}),400
        elif(check(num1,num2)):
            dic["created_by"]=request.get_json()["created_by"]
            dic["source"]=request.get_json()["source"]
            dic["destination"]=request.get_json()["destination"]
            time = request.get_json()["timestamp"]   
            arr=time.split(":")
            da=[]
            for i in arr:
                da.append(i.split('-'))
            pa = re.compile("^[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]\:[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9]$")
            try:
                if re.match(pa,time):
                    d  = datetime(int(da[0][2]),int(da[0][1]),int(da[0][0]),int(da[1][2]),int(da[1][1]),int(da[1][0])) 
                    dtime = d.strftime("%m/%d/%Y, %H:%M:%S")
                    dic['timestamp'] = dtime
                    #dic["timestamp"]= json.dumps(d, default = myconverter)
                    dic["some"] = "rides"
                    dic['riders'] = []
                    #dictt=json.dumps(dic, default = myconverter)
                    url="http://34.204.31.34:80/api/v1/db/write"
                    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
                    r = requests.post(url, json = dic, headers=headers)
                    return jsonify({}),201 
                else:
                    return jsonify({}),400       
            except ValueError:
                return jsonify({}),400
        else:
            return jsonify({}),400
    else:
        return jsonify({}),400

@app.route("/api/v1/rides", methods=["GET"])
def list_details():
    with counter.get_lock():
        counter.value+=1
    source = request.args.get('source')
    destination = request.args.get('destination')
    if(source==destination):
        return jsonify({}),400
    elif(check(source,destination)):
        dicto ={}
        dicto['source']  = source
        dicto['destination'] = destination
        dicto['some'] = 'list'
        url="http://34.204.31.34:80/api/v1/db/read"
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url,json = dicto ,headers = headers)
        if r.text=="None":
            return jsonify({}),204
        else:
            return r.text
    else:
        dictt={}
        dictt["some"] = "adduser1"
        url="http://34.204.31.34:80/api/v1/db/read"
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url, json = dictt, headers=headers)
        return jsonify({}),400
        

@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def get_details(rideId):
    with counter.get_lock():
        counter.value+=1
    #rideid = request.args.get('rideId')
    #ride=Ride.query.get(rideId)
    #if(ride):
    data = {}
    data['rideId'] = rideId
    data['some'] = 'ridesid'
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url,json=data,headers = headers)
    if(r.text=="None"):
        return jsonify({}),204
    else:
        dumb={}
        dit={}
        dit["some"]="get_details"
        dit["riders"]=r.json()['riders']
        url="http://34.204.31.34:80/api/v1/db/read"
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        res = requests.post(url,json=dit,headers = headers)
        r3 = res.text
        x=r3.strip("[")
        y=x.strip("]")
        lis = y.split(",")
        lis1=[]
        for i in lis:
            x1=i.strip()
            y=x1.strip("'")
            lis1.append(y)
        dumb["rideId"]=rideId
        dumb["created_by"]=r.json()["created_by"]
        dumb["users"]= lis1
        dumb["timestamp"]=converter(r.json()["timestamp"])
        dumb["source"]=str(r.json()["source"])
        dumb["destination"]=str(r.json()["destination"])
        return dumb


@app.route("/api/v1/rides/<rideId>",methods=["POST"])
def join_ride(rideId):
    with counter.get_lock():
        counter.value+=1
    username = request.json['username']
    data = {}
    data['rideId'] = rideId
    data['some'] = 'ridesid'
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url,json=data,headers = headers)
    if(r.text=="None"):
        return jsonify({}),400
    else:  
        url1="http://Load-balancer-142669967.us-east-1.elb.amazonaws.com:80/api/v1/users"
        headers = {'Origin': '34.204.31.34'}
        #url1="http://34.204.31.34:80/api/v1/users"
        r2 = requests.get(url1,headers = headers)
        r3 = r2.text
        x=r3.strip("[")
        y=x.strip("]")
        lis = y.split(",")
        #return type(lis)
        lis1=[]
        for i in lis:
            x1=i.strip()
            y=x1.strip("'")
            lis1.append(y)
        user=r.json()["created_by"]
        riders=r.json()["riders"]
        if username in lis1:
            if(username==user):
                return jsonify({}),400
            elif (username in riders) :
                return jsonify({}),400
            else:
                data['some'] = 'join_rides'
                data['rideId'] = rideId
                data['username'] = username
                url="http://34.204.31.34:80/api/v1/db/write"
                headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
                r = requests.post(url,json = data ,headers = headers)   
                return jsonify({}),200
        else:
            return jsonify({}),400
          
@app.route('/api/v1/rides/<rideId>', methods=['PUT'])
def delete_ride1(rideId):
    with counter.get_lock():
       counter.value += 1
    dictt={}
    dictt["some"] = "adduser1"
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, json = dictt, headers=headers)
    return jsonify({}), 405


@app.route("/api/v1/rides/<rideId>", methods=["DELETE"])
def ride_delete(rideId):
    with counter.get_lock():
        counter.value+=1
    dic = {}
    dic['rideId'] = rideId
    dic['some'] = "delete_ride"
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url,json = dic ,headers = headers)
    #return r.json
    x = r.json()
    if str(x['exists'])=="None":
        return jsonify({}),400
    else:
        url="http://34.204.31.34:80/api/v1/db/write"
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        res = requests.post(url, json = dic, headers=headers)
        return jsonify({}),200
        #return res.text


@app.route("/api/v1/rides/count",methods=["GET"])
def rides_count():
    dic={}
    with counter.get_lock():
        counter.value+=1
    url="http://34.204.31.34:80/api/v1/db/read"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    dic['some'] = 'ride_count'
    res = requests.post(url, json = dic, headers=headers)
    return res.text
    #return str(li)


        
    
        
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

