from flask import Flask
from flask import request
from flask import jsonify
from flask import url_for
from flask import redirect
import json
from datetime import datetime
import pika
import uuid
import docker
import time
import threading

app = Flask(__name__)
client = docker.from_env()
number_of_reads = 0

sleepTime = 20
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))
write_channel = connection.channel()
write_channel.queue_declare(queue='WRITEQ', durable=True)
correl_id = 0

class Read_Object:
	def __init__(self):
		global correl_id
		self.corr_id = str(correl_id)
		correl_id += 1

		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='READQ', durable=True)
		self.callback_queue = 'RESPONSEQ'

		self.channel.basic_consume(
			queue=self.callback_queue,
			on_message_callback=self.on_response)
		#self.start_consuming()

	def on_response(self, ch, method, props, body):
		#print(self.corr_id, props.correlation_id)
		if self.corr_id == props.correlation_id:
			self.response = json.loads(body)
			ch.basic_ack(delivery_tag=method.delivery_tag)
			self.connection.close()

	def call(self, query, columns):
		self.response = None
		self.channel.basic_publish(
			exchange='',
			routing_key='READQ',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=query)
		while self.response is None:
			self.connection.process_data_events()
			#pass
		dic = {}
		if(self.response):
			if(len(self.response) == 1):
				for tu in self.response:
					if(len(columns) == len(tu)):
						for i in range(len(tu)):
							dic[columns[i]] = tu[i]
					else:
						dic[columns[0]] = tu[0]
				return jsonify(dic)
			else:
				li = []
				for tu in self.response:
					dic = {}
					if(len(columns) == len(tu)):
						for i in range(len(tu)):
							dic[columns[i]] = tu[i]
					else:
						dic[columns[0]] = tu[0]
					li.append(dic)
				return jsonify(li)
		else:
			return jsonify({})

def create_new_slave():
	client.containers.run("test_slave:latest", network = "test_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"])

def deploy_slaves():
	global number_of_reads
	curr_num_containers = 0
	for container in client.containers.list():
		print(container.image,type(container.image))
		if(str(container.image) == "<Image: 'test_slave:latest'>"):
			curr_num_containers += 1
	print("Number of slaves : "+str(curr_num_containers), flush=True)
	print("Number of reads : "+str(number_of_reads))
	if(curr_num_containers > ((number_of_reads-1)//20)+1):
		for container in client.containers.list():
			if((str(container.image) == "<Image: 'test_slave:latest'>") and curr_num_containers>((number_of_reads-1)//20)+1 and not(curr_num_containers==1)):
				print("Stopping Container")
				container.kill()
				curr_num_containers -= 1
			elif(curr_num_containers==((number_of_reads-1)//20)+1):
				break
		number_of_reads = 0
	elif(curr_num_containers < ((number_of_reads-1)//20)+1):
		no_of_reads = number_of_reads
		number_of_reads = 0
		for i in range(curr_num_containers,((no_of_reads-1)//20)+1):
			print("Creating New Slave")
			t = threading.Thread(target = create_new_slave)
			t.start()
			#print(container.logs)
		for container in client.containers.list():
			print(container.image,type(container.image))
		#number_of_reads = 0

'''def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t'''

'''def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t'''

def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t

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
	if(correl_id == 0):
		set_interval(deploy_slaves, 120)
	global number_of_reads
	number_of_reads += 1
	print(number_of_reads)
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
		else:
			sql = "SELECT "+column_string+" FROM "+tabname
		read_request = Read_Object()
		#print(read_request.corr_id)
		read_response = read_request.call(sql,columns)
		return read_response, 200

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

#set_interval(deploy_slaves, 40)
app.run(debug=True, host='0.0.0.0', port=80)
#t.join()
