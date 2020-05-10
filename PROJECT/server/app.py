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


app = Flask(__name__)
client = docker.from_env()
number_of_reads = 0
sync_messages = []

sleepTime = 200
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))
write_channel = connection.channel()
write_channel.queue_declare(queue='WRITEQ', durable=True)
correl_id = 0
is_first_write = 1
is_scaling = 0
is_first_time = 1

zk = KazooClient(hosts='Zookeeper:2181')
zk.start()

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
if(zk.exists("/workers/slaves")):
	zk.delete("/workers/slaves", recursive=True)
	zk.delete("/workers/master", recursive=True)
zk.ensure_path("/workers/slaves")
zk.ensure_path("/workers/master")

def is_master(container):
	'''command = shlex.split("docker exec "+str(container.id)+" bash -c \'echo \"$IS_MASTER\"\'")
	try:
		output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
		success = True 
	except subprocess.CalledProcessError as e:
		output = e.output.decode()
		success = False'''
	#return str(output)
	#return type(output)
	#return [output,'True',output=='True']
	if("MASTER" in container.name or "master" in container.name):
		return True
	else:
		return False

def update_zookeeper():
	print("Entered Update Zookeeper")
	while(True):
		global zk
		global is_scaling
		global is_first_time
		if(is_first_time == 1):
			for container in client.containers.list():
				if(str(container.image) == "<Image: 'test2_slave:latest'>" and not(is_master(container))):
					#zk.ensure_path("/workers/slaves/"+str(container.name))
					zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
				elif(str(container.image) == "<Image: 'test2_master:latest'>"):
					#zk.ensure_path("/workers/master/"+str(container.name))
					zk.create("/workers/master/"+str(container.name), str(container.id).encode('utf-8'))
			is_first_time = 0
		elif(is_scaling == 0):
			slave_names = [str(container.name) for container in client.containers.list() if str(container.image) == "<Image: 'test2_slave:latest'>" and not(is_master(container))]
			for zk_name in zk.get_children("/workers/slaves"):
				if zk_name not in slave_names:
					if(zk.exists("/workers/slaves/"+str(zk_name))):
						print("ZOOKEEPER OBSERVED THAT SLAVE "+zk_name+" FAILED")
						zk.delete("/workers/slaves/"+str(zk_name))
						container = client.containers.run("test2_slave:latest", network = "test2_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
						zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
						print("ZOOKEEPER CREATED SLAVE "+container.name)
						#zk.ensure_path("/workers/slaves/"+str(zk_name)+"-FAILED")
						'''if(not(zk.exists("/workers/slaves/"+str(zk_name)+"-FAILED"))):
							zk.create("/workers/slaves/"+str(zk_name)+"-FAILED", str(data).encode('utf-8'))'''
			master_name = [str(container.name) for container in client.containers.list() if is_master(container)]
			mutex = 0
			for zk_name in zk.get_children("/workers/master"):
				if zk_name not in master_name:
					if(zk.exists("/workers/master/"+str(zk_name)) and ("master" in str(zk_name) or "MASTER" in str(zk_name)) and mutex==0):
						mutex = 1
						print("ZOOKEEPER OBSERVED THAT MASTER FAILED")
						zk.delete("/workers/master/"+str(zk_name))
						#zk.ensure_path("/workers/master/"+str(zk_name)+"-FAILED")
						'''if(not(zk.exists("/workers/master/"+str(zk_name)+"-FAILED"))):
							zk.create("/workers/master/"+str(zk_name)+"-FAILED", str(data).encode('utf-8'))'''
						min_pid = 100000
						min_container = None
						for container in client.containers.list():
							if(str(container.image) == "<Image: 'test2_slave:latest'>" and not(is_master(container))):
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
						#subprocess.call("docker exec -i "+str(min_container.id)+" /bin/bash -c \"export IS_MASTER=True\"", shell=True)
						#print(min_container.exec_run(cmd = "ps", environment = ["IS_MASTER=True"]))
						zk.delete("/workers/slaves/"+str(min_container.name))
						min_container.rename(min_container.name+"-MASTER")
						zk.create("/workers/master/"+str(min_container.name)+"-MASTER", str(min_container.id).encode('utf-8'))
						print("ZOOKEEPER ELECTED NEW MASTER : ",str(min_container.name)+"-MASTER")
						container = client.containers.run("test2_slave:latest", network = "test2_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
						zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
						print("ZOOKEEPER CREATED SLAVE "+container.name)
						mutex = 0

def callback_sync(ch, method, properties, body):
	global sync_messages
	content = body.decode()
	sync_messages.append(content)
	ch.basic_ack(delivery_tag=method.delivery_tag)

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
					
@zk.ChildrenWatch("/workers/slaves")
def watch_slave(children):
	print("Zookeeper Slaves : ",children)

@zk.ChildrenWatch("/workers/master")
def watch_master(children):
	print("Zookeeper Master : ",children)		

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

'''def create_new_slave():
	client.containers.run("test2_slave:latest", network = "test2_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"])'''

def deploy_slaves():
	global number_of_reads
	global is_scaling
	is_scaling = 1
	curr_num_containers = 0
	for container in client.containers.list():
		print(container.image,type(container.image))
		if(str(container.image) == "<Image: 'test2_slave:latest'>" and not(is_master(container))):
			curr_num_containers += 1
	print("Number of slaves : "+str(curr_num_containers), flush=True)
	print("Number of reads : "+str(number_of_reads))
	if(curr_num_containers > ((number_of_reads-1)//20)+1):
		for container in client.containers.list():
			if(str(container.image) == "<Image: 'test2_slave:latest'>" and not(is_master(container)) and curr_num_containers>((number_of_reads-1)//20)+1 and not(curr_num_containers==1)):
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
			'''t = threading.Thread(target = create_new_slave)
			t.start()'''
			container = client.containers.run("test2_slave:latest", network = "test2_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
			zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
			#print(container.logs)
		for container in client.containers.list():
			print(container.image,type(container.image))
		#number_of_reads = 0
	number_of_reads = 0
	is_scaling = 0

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

@app.route('/api/v1/crash/master', methods=['POST'])
def crash_master():
	for container in client.containers.list():
		#if(str(container.image) == "<Image: 'test2_master:latest'>"):
			#return str(is_master(container)), 200
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

@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
	max_pid = 0
	max_container = None
	for container in client.containers.list():
		if(str(container.image) == "<Image: 'test2_slave:latest'>" and not(is_master(container))):
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

@app.route('/api/v1/worker/list', methods=['GET'])
def worker_list():
	workers_list = []
	for container in client.containers.list():
		if(str(container.image) == "<Image: 'test2_slave:latest'>" or str(container.image) == "<Image: 'test2_master:latest'>"):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			workers_list.append(int(output))
	return jsonify(sorted(workers_list)), 200

@app.route('/api/v1/syncq/content', methods=['GET'])
def get_syncq_content():
	global sync_messages
	return jsonify(sync_messages), 200

if __name__=='__main__':
	app.run(debug=True, host='0.0.0.0', port=80)
