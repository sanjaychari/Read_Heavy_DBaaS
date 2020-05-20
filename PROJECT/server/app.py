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
import shlex
import subprocess
from kazoo.client import KazooClient, KazooState

#Setting Up Flask App context
app = Flask(__name__)
#Setting up Docker client to connect to localhost's docker
client = docker.from_env()

#Global Variables
number_of_reads = 0
sync_messages = []
correl_id = 0
is_first_write = 1
is_scaling = 0
is_first_time = 1
sleepTime = 20

#Making the app sleep in order to wait for RabbitMQ and Zookeeper to start up
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

#Creating a connection to WRITEQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))
write_channel = connection.channel()
write_channel.queue_declare(queue='WRITEQ', durable=True)

#Setting up Zookeeper client to connect to Zookeeper container
zk = KazooClient(hosts='Zookeeper:2181')
zk.start()

#Listening for state of zookeeper connection
def my_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        print("Connection to Zookeeper lost")
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        print("Zookeeper Disconnected")
    else:
        # Handle being connected/reconnected to Zookeeper
        print("Zookeeper Connected Sucessfully")
zk.add_listener(my_listener)

#Deleting existing zookeeper paths and creating new ones
if(zk.exists("/workers/slaves")):
	zk.delete("/workers/slaves", recursive=True)
	zk.delete("/workers/master", recursive=True)
zk.ensure_path("/workers/slaves")
zk.ensure_path("/workers/master")

#Function to check if a worker is master or slave
def is_master(container):
	if("MASTER" in container.name or "master" in container.name):
		return True
	else:
		return False

#Manager thread that handles creation and deletion of znodes according to the list of containers 
#currently running
def update_zookeeper():
	print("Entered Update Zookeeper")
	while(True):
		global zk
		global is_scaling
		global is_first_time
		if(is_first_time == 1):
			for container in client.containers.list():
				if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
					zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
				elif(str(container.image) == "<Image: 'project_master:latest'>"):
					zk.create("/workers/master/"+str(container.name), str(container.id).encode('utf-8'))
			is_first_time = 0
		elif(is_scaling == 0):
			slave_names = [str(container.name) for container in client.containers.list() if str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))]
			for zk_name in zk.get_children("/workers/slaves"):
				if zk_name not in slave_names:
					if(zk.exists("/workers/slaves/"+str(zk_name))):
						print("ZOOKEEPER OBSERVED THAT SLAVE "+zk_name+" FAILED")
						zk.delete("/workers/slaves/"+str(zk_name))
						container = client.containers.run("project_slave:latest", network = "project_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
						zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
						print("ZOOKEEPER CREATED SLAVE "+container.name)
			master_name = [str(container.name) for container in client.containers.list() if is_master(container)]
			mutex = 0
			for zk_name in zk.get_children("/workers/master"):
				if zk_name not in master_name:
					if(zk.exists("/workers/master/"+str(zk_name)) and ("master" in str(zk_name) or "MASTER" in str(zk_name)) and mutex==0):
						mutex = 1
						print("ZOOKEEPER OBSERVED THAT MASTER FAILED")
						zk.delete("/workers/master/"+str(zk_name))
						min_pid = 100000
						min_container = None
						for container in client.containers.list():
							if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
								command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
								try:
									output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
									success = True 
								except subprocess.CalledProcessError as e:
									output = e.output.decode()
									success = False
								if int(output) < min_pid:
									min_pid = int(output)
									min_container = container
						zk.delete("/workers/slaves/"+str(min_container.name))
						min_container.rename(min_container.name+"-MASTER")
						zk.create("/workers/master/"+str(min_container.name)+"-MASTER", str(min_container.id).encode('utf-8'))
						print("ZOOKEEPER ELECTED NEW MASTER : ",str(min_container.name)+"-MASTER")
						container = client.containers.run("project_slave:latest", network = "project_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
						zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
						print("ZOOKEEPER CREATED SLAVE "+container.name)
						mutex = 0

#Function that appends messages consumed from the SYNCQ into a global list. 
#This global list is maintained for database consistency in new slaves
def callback_sync(ch, method, properties, body):
	global sync_messages
	content = body.decode()
	sync_messages.append(content)
	ch.basic_ack(delivery_tag=method.delivery_tag)

#Thread that consumes from the SYNCQ
def consume_from_syncq():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
	sync_channel = connection.channel()
	sync_channel.exchange_declare(exchange='SYNCEXCHANGE', exchange_type='fanout')
	sync_channel.queue_declare(queue='SYNCQ', durable=True)
	queue_name = "SYNCQ"
	sync_channel.queue_bind(exchange='SYNCEXCHANGE', queue=queue_name)
	print("Consumption from SYNCQ beginning")
	sync_channel.basic_consume(queue=queue_name, on_message_callback=callback_sync)
	sync_channel.start_consuming()
					
#Built in Kazoo watcher API that is triggered whenever the list of slave znodes changes
@zk.ChildrenWatch("/workers/slaves")
def watch_slave(children):
	print("Zookeeper Slaves : ",children)

#Built in Kazoo watcher API that is triggered whenever the list of master znodes changes
@zk.ChildrenWatch("/workers/master")
def watch_master(children):
	print("Zookeeper Master : ",children)		

#Object that is created when the read API is called
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

	def on_response(self, ch, method, props, body):
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

#Manager thread for scaling in and scaling out of slave containers
def deploy_slaves():
	global number_of_reads
	global is_scaling
	is_scaling = 1
	curr_num_containers = 0
	for container in client.containers.list():
		print(container.image,type(container.image))
		if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
			curr_num_containers += 1
	print("Number of slaves : "+str(curr_num_containers), flush=True)
	print("Number of reads : "+str(number_of_reads))
	if(curr_num_containers > ((number_of_reads-1)//20)+1):
		for container in client.containers.list():
			if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container)) and curr_num_containers>((number_of_reads-1)//20)+1 and not(curr_num_containers==1)):
				print("Stopping Container")
				zk.delete("/workers/slaves/"+str(container.name))
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
			container = client.containers.run("project_slave:latest", network = "project_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
			zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
		for container in client.containers.list():
			print(container.image,type(container.image))
	number_of_reads = 0
	is_scaling = 0

#Function that sets a recurring interval to call another function
def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t

#Check if string can be converted to a number
def isnumeric(string):
	try:
		num = int(string)
		return True
	except:
		return False

#Check if string is SHA1 form
def check_sha1(string):
	if(len(string)!=40):
		return False
	for i in string:
		if(not(i=='a' or i=='b' or i=='c' or i=='d' or i=='e' or i=='f' or i=='A' or i=='B' or i=='C' or i=='D' or i=='E' or i=='F' or (isnumeric(i) and int(i)>=0 and int(i)<=9))):
			return False
	return True

#Check if string is in valid Datetime format
def validate_date(datestr):
	try:
		now = datetime.strptime(datestr,'%d-%m-%Y:%S-%M-%H')
		return True
	except:
		return False

#Read DB API
@app.route('/api/v1/db/read', methods=['POST'])
def read_data():
	if(correl_id == 0):
		t = threading.Thread(target = update_zookeeper)
		t.start()
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

#Write DB API
@app.route('/api/v1/db/write', methods=['POST'])
def write_data():
	global is_first_write
	global correl_id
	if(is_first_write == 1 and correl_id == 0):
		t1 = threading.Thread(target = update_zookeeper)
		t1.start()
	elif(is_first_write == 1):
		t2 = threading.Thread(target = consume_from_syncq)
		t2.start()
	is_first_write = 0
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

#Crashes the master container
@app.route('/api/v1/crash/master', methods=['POST'])
def crash_master():
	for container in client.containers.list():
		if(is_master(container)):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			master_pid = int(output)
			print("Killing Master")
			container.kill()
	return jsonify([master_pid]), 200

#Crashes the slave container with highest pid
@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
	max_pid = 0
	max_container = None
	for container in client.containers.list():
		if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			if int(output) > max_pid:
				max_pid = int(output)
				max_container = container
	print("Killing Slave")
	max_container.kill()
	return jsonify([max_pid]), 200

#Displays list of workers
@app.route('/api/v1/worker/list', methods=['GET'])
def worker_list():
	workers_list = []
	for container in client.containers.list():
		if(str(container.image) == "<Image: 'project_slave:latest'>" or str(container.image) == "<Image: 'project_master:latest'>"):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			workers_list.append(int(output))
	return jsonify(sorted(workers_list)), 200

#API to retrieve content of SYNCQ. This is called from newly spawned slaves to maintain database consistency.
@app.route('/api/v1/syncq/content', methods=['GET'])
def get_syncq_content():
	global sync_messages
	return jsonify(sync_messages), 200

#Run the app
if __name__=='__main__':
	app.run(debug=True, host='0.0.0.0', port=80)
