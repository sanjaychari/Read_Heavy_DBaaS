from flask import Flask
from flask import request
from flask import jsonify
from flask import url_for
from flask import redirect
import json
from datetime import datetime
import pika

class Read_Object:
	def __init__(self,query,corrid):
		self.query = query
		self.corrid = corrid
		self.response = None

app = Flask(__name__)
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))
write_channel = connection.channel()
write_channel.queue_declare(queue='WRITEQ', durable=True)
read_channel = connection.channel()
read_channel.queue_declare(queue='READQ', durable=True)
response_channel = connection.channel()
response_channel.queue_declare(queue='RESPONSEQ', durable=True)
read_request = {}
response_obj = []

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

#connection.close()
req_no = 0

def check_response_queue(ch, method, properties, body):
	#print(" [x] Received %s" % body)
	global read_request
	global response_obj
	content = json.loads(body)
	read_request = json.loads(read_request)
	if(read_request['corr_id'] == content['corr_id']):
		response_obj = content['response']
		ch.basic_ack(delivery_tag=method.delivery_tag)
		response_channel.stop_consuming()

@app.route('/api/v1/db/read', methods=['POST'])
def read_data():
	global req_no
	global read_request
	global response_obj
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
			sql = "SELECT "+column_string+" FROM "+tabname+" WHERE "+where_string
			read_request = json.dumps({"corr_id" : req_no, "query" : sql})
			req_no += 1
		else:
			sql = "SELECT "+column_string+" FROM "+tabname
			read_request = json.dumps({"corr_id" : req_no, "query" : sql})
			req_no += 1
		read_channel.basic_publish(
			exchange='',
			routing_key='READQ',
			body=read_request,
			properties=pika.BasicProperties(
				delivery_mode=2,  # make message persistent
			))
		response_channel.basic_qos(prefetch_count=1)
		response_channel.basic_consume(queue='RESPONSEQ', on_message_callback=check_response_queue)
		response_channel.start_consuming()
		dic = {}
		if(response_obj):
			if(len(response_obj) == 1):
				for tu in response_obj:
					if(len(columns) == len(tu)):
						for i in range(len(tu)):
							dic[columns[i]] = tu[i]
					else:
						dic[columns[0]] = tu[0]
				return jsonify(dic), 200
			else:
				li = []
				for tu in response_obj:
					dic = {}
					if(len(columns) == len(tu)):
						for i in range(len(tu)):
							dic[columns[i]] = tu[i]
					else:
						dic[columns[0]] = tu[0]
					li.append(dic)
				return jsonify(li), 200
		else:
			return jsonify({}), 200

	elif(delete=="True"):
		if(not(where_string=="None")):
			sql = "DELETE FROM "+tabname+" WHERE "+where_string
			write_channel.basic_publish(
				exchange='',
				routing_key='WRITEQ',
				body=sql,
				properties=pika.BasicProperties(
					delivery_mode=2,  # make message persistent
				))
			return jsonify({}), 200
		else:
			sql = "DELETE FROM "+tabname
			write_channel.basic_publish(
				exchange='',
				routing_key='WRITEQ',
				body=sql,
				properties=pika.BasicProperties(
					delivery_mode=2,  # make message persistent
				))
			return jsonify({}), 200

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
		sql = "INSERT INTO "+tabname+" ("+column_string+") VALUES ("+value_string+")"
		write_channel.basic_publish(
				exchange='',
				routing_key='WRITEQ',
				body=sql,
				properties=pika.BasicProperties(
					delivery_mode=2,  # make message persistent
				))
		return jsonify({}),201
	elif(update=="True"):
		where_string = req_data['where']
		sql = "UPDATE "+tabname+" SET "+column_string+" = "+value_string+" WHERE "+where_string
		write_channel.basic_publish(
				exchange='',
				routing_key='WRITEQ',
				body=sql,
				properties=pika.BasicProperties(
					delivery_mode=2,  # make message persistent
				))
		return jsonify({}),201

'''@app.route('/')
def index():
	return 'OK'


@app.route('/add-job/<cmd>')
def add(cmd):
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	channel = connection.channel()
	channel.queue_declare(queue='task_queue', durable=True)
	channel.basic_publish(
		exchange='',
		routing_key='task_queue',
		body=cmd,
		properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
		))
	connection.close()
	return " [x] Sent: %s" % cmd'''

if __name__ == '__main__':
	app.run(debug=True, host='0.0.0.0')

