from sqlalch import db
from sqlalch import User
from sqlalch import Ride
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from flask import Flask
from flask import request
from flask import jsonify
from flask import url_for
from flask import redirect
import json
from sqlalchemy import text
import requests
import pandas as pd


app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] 	= 'sqlite:///assignment2.db'
#app.config['SQLALCHEMY_DATABASE_URI'] 	= 'sqlite:////home/ubuntu/CC_Assignment1/test.db'
db.init_app(app)

def isnumeric(string):
	try:
		num = int(string)
		return True
	except:
		return False

def check_sha1(string):
	if(len(string)!=40):
		return False
	for i in string:
		if(not(i=='a' or i=='b' or i=='c' or i=='d' or i=='e' or i=='f' or i=='A' or i=='B' or i=='C' or i=='D' or i=='E' or i=='F' or (isnumeric(i) and int(i)>=0 and int(i)<=9))):
			return False
	return True

def validate_date(datestr):
	try:
		now = datetime.strptime(datestr,'%d-%m-%Y:%S-%M-%H')
		return True
	except:
		return False


@app.route('/api/v1/db/read', methods=['POST'])
def read_data():
    req_data = request.get_json()

    tabname = req_data['table']
    columns = req_data['columns']
    where = req_data['where']
    delete = req_data['delete']
    if(not(where=="None")):
        wcol = where.split("=")[0]
        wval = where.split("=")[1]
        where_string=""
        for i in range(len(where.split("AND"))):
            if(not(isnumeric(where.split("AND")[i].split("=")[1]))):
                where_string+=where.split("AND")[i].split("=")[0]
                where_string+="=\'"
                where_string+=where.split("AND")[i].split("=")[1]
                where_string+="\'"
            else:
                where_string+=where.split("AND")[i].split("=")[0]
                where_string+="="
                where_string+=where.split("AND")[i].split("=")[1]
            if(not(i==len(where.split("AND"))-1)):
        	    where_string+=" AND "
    else:
        where_string = "None"

    if(isinstance(columns,list)):
    	column_string = ""
    	for s in range(len(columns)):
    		if(s!=len(columns)-1):
    			column_string+=columns[s]+","
    		else:
    			column_string+=columns[s]
    else:
    	column_string = columns

    if(delete == "False"):
        if(not(where_string=="None")):
            sql = text("SELECT "+column_string+" FROM "+tabname+" WHERE "+where_string)
        else:
        	sql = text("SELECT "+column_string+" FROM "+tabname)
        result = db.session.execute(sql)
        li = []
        dic = {}
        nor = 0
        for row in result:
            nor += 1
            dic = {}
            for i in range(len(row)):
                if(columns[i] not in dic):
                    dic[columns[i]]=row[i]
            li.append(dic)
        if(nor == 1 or nor==0):
            return jsonify(dic)
        else:
            return jsonify(li)

    elif(delete=="True"):
    	if(not(where_string=="None")):
    		sql = text("DELETE FROM "+tabname+" WHERE "+where_string)
    		result = db.session.execute(sql)
    		db.session.commit()
    		return jsonify({})
    	else:
    		sql = text("DELETE FROM "+tabname)
    		result = db.session.execute(sql)
    		db.session.commit()
    		return jsonify({})



@app.route('/api/v1/db/write', methods=['POST'])
def write_data():
	req_data = request.get_json()
	values = req_data['insert']
	columns = req_data['column']
	tabname = req_data['table']
	update = req_data['update']
	column_string = ""
	for s in range(len(columns)):
		if(s!=len(columns)-1):
			column_string+=columns[s]+","
		else:
			column_string+=columns[s]
	value_string = ""
	for s in range(len(values)):
		if(s!=len(values)-1):
			if(isnumeric(values[s])):
				value_string += values[s] + ","
			else:
				value_string += "\'" + values[s] + "\'" + ","
		else:
			if(isnumeric(values[s])):
				value_string += values[s]
			else:
				value_string += "\'" + values[s] + "\'"
	if(update=="False"):
		sql = text("INSERT INTO "+tabname+" ("+column_string+") VALUES ("+value_string+")")
		try:
			db.session.execute(sql)
			db.session.commit()
			return jsonify({}),201
		except:
			db.session.rollback()
			return jsonify({}),400
	elif(update=="True"):
		where_string = req_data['where']
		sql = text("UPDATE "+tabname+" SET "+column_string+" = "+value_string+" WHERE "+where_string)
		db.session.execute(sql)
		try:
			db.session.execute(sql)
			db.session.commit()
			return jsonify({}),200
		except:
			db.session.rollback()
			return jsonify({}),204

#Add users - API 1
@app.route('/api/v1/users', methods=['PUT'])
def add_user():
	if(not(request.method=="PUT")):
		return jsonify({}),405
	try:
		req_data = request.get_json()
		username = req_data['username']
		password = req_data['password']
	except:
		return jsonify({}),400
	if(not(check_sha1(password))):
		return jsonify({}),400
	resp_data = {'insert':[username,password],'column':["username","password"],'table':"user","update":"False"}
	response = requests.post("http://127.0.0.1:5000/api/v1/db/write",json=resp_data)
	return jsonify({}),response.status_code

#Remove users - API 2
@app.route('/api/v1/users/<name>', methods=['DELETE'])
def remove_user(name):
	if(not(request.method=="DELETE")):
		return jsonify({}),405
	resp_data = {'table':"user",'columns':["username"],"where":"username="+name,"delete":"False"}
	response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
	if(response.text=="{}\n"):
		return jsonify({}),400
	resp_data = {'table':"user",'columns':"username","where":"username="+name,"delete":"True"}
	response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
	resp_data = {'table':"ride",'columns':"created_by","where":"created_by="+name,"delete":"True"}
	response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
	resp_data = {'table':"ride",'columns':["rideId","users"],"where":"None","delete":"False"}
	response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
	#return response.text
	#list_r = response.text.strip('][').split('{')
	#return str(list_r)
	#return str(type(json.loads(response.text)))
	if(response.text == "{}\n"):
		return jsonify({}),200
	res = json.loads(response.text)
	if(isinstance(res,list)):
		for dic in res:
			users_list = []
			for s in dic["users"][1:-1].split(", "):
				if(not(s==name)):
					users_list.append(s)
			print(users_list)
			resp_data = {'insert':['[%s]' % ', '.join(map(str, users_list))],'column':["users"],'table':"ride","update":"True","where":"rideId="+str(dic["rideId"])}
			response = requests.post("http://127.0.0.1:5000/api/v1/db/write",json=resp_data)
	else:
		users_list = []
		for s in res["users"][1:-1].split(", "):
			if(not(s==name)):
				users_list.append(s)
		print(users_list)
		resp_data = {'insert':['[%s]' % ', '.join(map(str, users_list))],'column':["users"],'table':"ride","update":"True","where":"rideId="+str(res["rideId"])}
		response = requests.post("http://127.0.0.1:5000/api/v1/db/write",json=resp_data)
	return jsonify({}),200

@app.route('/api/v1/users', methods=['GET'])
def list_all_users():
	if(not(request.method=="GET")):
		return jsonify([]),405
	resp_data = {'table':"user",'columns':["username"],"where":"None","delete":"False"}
	response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
	if(response.text == "{}\n"):
		return jsonify([]),204
	resp = json.loads(response.text)
	resp_list = []
	if(isinstance(resp,list)):
		for i in resp:
			resp_list.append(i["username"])
	else:
		resp_list.append(resp["username"])
	return jsonify(resp_list),200

@app.route('/api/v1/db/clear', methods=['POST'])
def clear_db():
	if(not(request.method)=="POST"):
		return jsonify({}),405
	try:
		resp_data = {'table':"ride",'columns':["created_by"],"where":"None","delete":"True"}
		response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
		resp_data = {'table':"user",'columns':["username"],"where":"None","delete":"True"}
		response = requests.post("http://127.0.0.1:5000/api/v1/db/read",json=resp_data)
		return jsonify({}),200
	except:
		return jsonify({}),400

if __name__ == "__main__":
	app.run(debug=True)