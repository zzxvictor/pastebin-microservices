from flask import Flask
import boto3
import redis
import pika
import logging


app = Flask(__name__)
redis_client = redis.Redis(host='caching', port=6379)
mq_client = pika.BlockingConnection(pika.ConnectionParameters('mq'))
channel = mq_client.channel()
channel.queue_declare(queue='hello')


@app.route('/hello')
def hello():
    redis_client.set('foo', 'bar2')
    value = redis_client.get('foo')
    # res = channel.basic_publish(exchange='',
    #                       routing_key='hello',
    #                       body='Hello World!')
    # 
    # def callback(ch, method, properties, body):
    #     logging.warning(body)
    #
    # channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
    # channel.start_consuming()

    return '1243'


if __name__ == '__main__':

    app.run(debug=True)
