from flask import Flask, request, jsonify
from paste_controller import UploadController, RetrieveController
import boto3
import logging
import config
import redis
import pika


logging.basicConfig(level=config.LOGGING_LEVEL)
app = Flask(__name__)
db_client = boto3.client('dynamodb', region_name='us-east-1')
redis_client = redis.Redis(host='caching', port=6379)
mq_client = pika.BlockingConnection(pika.ConnectionParameters('mq'))
mq_client.channel().queue_declare(queue=config.QUEUE, durable=True)


@app.route('/upload', methods=['POST'])
def paste():
    payload = request.get_json()
    encryption_key = payload['encryption_key']
    user_data = payload['content']
    assert len(user_data) < config.MAX_PASTE_SIZE, 'maximum data size exceeded'
    uri = UploadController.handle_request(encryption_key,
                                          user_data,
                                          db_client=db_client,
                                          redis_client=redis_client,
                                          mq_client=mq_client)
    return {'statusCode': 200, 'body': uri}


@app.route('/retrieve/<uid>', methods=['POST'])
def copy(uid=''):
    assert uid != '', 'invalid uid'
    payload = request.get_json()
    data = RetrieveController.handle_request(uid, payload['password'],
                                             redis_client=redis_client, db_client=db_client)
    if data is None:
        status = 404
        data = 'The uri is either expired or not valid'
    else:
        status = 200
    return {'statusCode': status, 'body': data}


if __name__ == '__main__':
    app.run(debug=True)
