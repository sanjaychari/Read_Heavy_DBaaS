import pika
import time
import json
import sqlite3

conn = sqlite3.connect('database.db')
db = conn.cursor()

sleepTime = 20
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
write_channel = connection.channel()
write_channel.queue_declare(queue='WRITEQ', durable=True)
sync_channel = connection.channel()
sync_channel.queue_declare(queue='SYNCQ', durable=True)


print(' [*] Waiting for messages.')


def callback_write(ch, method, properties, body):
	print(" [x] Received %s" % body)
	content = body.decode()
	result = db.execute(content)
	conn.commit()
	#conn.close()
	sync_channel.basic_publish(
		exchange='',
		routing_key='SYNCQ',
		body= content,
		properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
		))
	ch.basic_ack(delivery_tag=method.delivery_tag)

write_channel.basic_qos(prefetch_count=1)
write_channel.basic_consume(queue='WRITEQ', on_message_callback=callback_write)
write_channel.start_consuming()

