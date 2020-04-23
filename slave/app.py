import pika
import time
import json
import sqlite3
import threading

threads = []

class Response_Object:
	def __init__(self,response,corr_id):
		self.response = response
		self.corr_id = corr_id

sleepTime = 40
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

print(' [*] Connecting to server ...')

print(' [*] Waiting for messages.')


def callback_read(ch, method, properties, body):
	conn = sqlite3.connect('database.db')
	db = conn.cursor()
	print(" [x] Received %s" % body)
	#content = json.loads(body)
	#cmd = content['query']
	#req_no = content['corr_id']
	cmd = body.decode()
	result = db.execute(cmd)
	response = result.fetchall()
	print(response,properties.correlation_id)
	#responseobj = {"corr_id" : req_no, "response" : response}
	#connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	#response_channel = connection.channel()
	#response_channel.queue_declare(queue='RESPONSEQ', durable=True)
	ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = properties.correlation_id),
                     body=json.dumps(response))
	ch.basic_ack(delivery_tag=method.delivery_tag)
	conn.close()
	#connection.close()


def callback_sync(ch, method, properties, body):
	conn = sqlite3.connect('database.db')
	db = conn.cursor()
	print(" [x] Received %s" % body)
	content = body.decode()
	result = db.execute(content)
	conn.commit()
	conn.close()
	ch.basic_ack(delivery_tag=method.delivery_tag)

def read_consume():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
	read_channel = connection.channel()
	read_channel.queue_declare(queue='READQ', durable=True)
	read_channel.basic_qos(prefetch_count=1)
	read_channel.basic_consume(queue='READQ', on_message_callback=callback_read)
	read_channel.start_consuming()

def sync_consume():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
	sync_channel = connection.channel()
	sync_channel.queue_declare(queue='SYNCQ', durable=True)
	sync_channel.basic_qos(prefetch_count=1)
	sync_channel.basic_consume(queue='SYNCQ', on_message_callback=callback_sync)
	sync_channel.start_consuming()

def manager():
	t1 = threading.Thread(target=read_consume)
	t1.daemon = True
	threads.append(t1)
	t1.start()  

	t2 = threading.Thread(target=sync_consume)
	t2.daemon = True
	threads.append(t2)


	t2.start()
	for t in threads:
		t.join()

manager()

