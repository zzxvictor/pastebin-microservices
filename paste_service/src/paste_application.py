from flask import Flask, request, jsonify
from paste_controller import UploadController, RetrieveController, UriNotFoundError, InvalidUriError
import boto3
import logging
import config
import redis
import pika
from flask_limiter import Limiter, RateLimitExceeded
from flask_limiter.util import get_remote_address


logging.basicConfig(level=config.LOGGING_LEVEL)
app = Flask(__name__)

limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

db_client = boto3.client('dynamodb', region_name='us-east-1')

redis_client = redis.Redis(host='caching', port=6379)

mq_client = pika.BlockingConnection(pika.ConnectionParameters('mq'))

mq_client.channel().queue_declare(queue=config.QUEUE, durable=True)


@app.errorhandler(Exception)
def base_exception_handler(error):
    print(error, flush=True)
    return 'Internal Server Error, please retry', 500


@app.errorhandler(RateLimitExceeded)
def rate_limited_handler(error):
    return 'Too many requests in a second', 429


@app.errorhandler(UriNotFoundError)
def uri_not_found_handler(error):
    return error.format_msg(), 404


@app.errorhandler(InvalidUriError)
def uri_not_valid_handler(error):
    return error.format_msg(), 404


@app.route('/upload', methods=['POST'])
@limiter.limit('2/second', override_defaults=True)
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
@limiter.limit('30/minute', override_defaults=True)
def copy(uid=''):
    assert uid != '', 'invalid uid'
    payload = request.get_json()
    data = RetrieveController.handle_request(uid, payload['password'],
                                             redis_client=redis_client, db_client=db_client)
    return {'statusCode': 200, 'body': data}


if __name__ == '__main__':
    app.run(debug=True)
