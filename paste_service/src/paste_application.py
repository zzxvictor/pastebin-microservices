from flask import Flask, request, jsonify
from paste_controller import UploadController, RetrieveController, UriNotFoundError, InvalidUriError, ContentTooBigError
import boto3
import logging
import config
import redis
import pika
from flask_limiter import Limiter, RateLimitExceeded
from flask_limiter.util import get_remote_address
from py_zipkin.zipkin import zipkin_span, create_http_headers_for_new_span, ZipkinAttrs, Kind, zipkin_client_span
from py_zipkin.request_helpers import create_http_headers
from py_zipkin.encoding import Encoding
import requests

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


@app.errorhandler(ContentTooBigError)
def content_too_big_handler(error):
    return error.format_msg(), 413


@app.route('/debug', methods=['GET'])
def hello():
    """For debugging"""
    return "server is running"


def default_handler(encoded_span):
    body = encoded_span
    return requests.post(
        "http://zipkin:9411/api/v2/spans",
        data=body,
        headers={'Content-Type': 'application/json'},
    )


@app.route('/upload', methods=['POST'])
@limiter.limit('2/second', override_defaults=True)
def paste():
    with zipkin_span(
            service_name='paste-bin',
            span_name='upload',
            port=8080,
            transport_handler=default_handler,
            sample_rate=100,  # Value between 0.0 and 100.0,
            encoding=Encoding.V2_JSON
    ):

        payload = request.get_json()

        encryption_key = payload['encryption_key']
        user_data = payload['content']
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
