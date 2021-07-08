import json
import logging
import time

import boto3
import config
import random
import string
from typing import Any
import datetime
from decimal import Decimal, ROUND_UP
import pika


def get_used_keys(db=None):
    if not db:
        db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(config.TABLE_NAME)
    scan_kwargs = {'AttributesToGet': ['uid']}
    done, start_key = False, None
    used_keys = set()
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        used_keys.update(set([item['uid'] for item in response['Items']]))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return used_keys


def key_gen(used: set, num: int=100) -> set:
    choices = string.ascii_lowercase + string.ascii_uppercase + string.digits
    new_keys = set()
    life_span = datetime.timedelta(days=config.LIFE_SPAN)
    for i in range(num):
        key = ''.join(random.choices(choices, k=config.KEY_LENGTH))
        if key not in new_keys and key not in used:
            expire_on = datetime.datetime.utcnow() + life_span
            expire_on = Decimal(expire_on.timestamp()).quantize(Decimal('1.'), rounding=ROUND_UP)
            new_keys.add((key, expire_on))
    return new_keys


def batch_writer(new_keys: set, db: Any=None) -> None:
    if not db:
        db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(config.TABLE_NAME)
    with table.batch_writer() as writer:
        for (key, expire_time) in new_keys:
            item = {'uid': key, 'expire_on': expire_time}
            writer.put_item(Item=item)


def check_que_length() -> int:
    params = pika.ConnectionParameters(host='mq')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    queue = channel.queue_declare(queue=config.QUEUE, durable=True)
    msg_count = queue.method.message_count
    logging.warning('message in the queue: {}'.format(msg_count))
    return msg_count


def send_to_que(msgs: set) -> None:
    params = pika.ConnectionParameters(host='mq')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    queue = channel.queue_declare(queue=config.QUEUE, durable=True)
    for msg in msgs:
        channel.basic_publish(exchange='', routing_key=config.QUEUE,
                              body=str.encode(json.dumps({'uid': msg[0],
                                                          'expire_on': float(msg[1])})))
    channel.close()
    logging.warning('messages sent: #{}'.format(len(msgs)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    while True:
        count = check_que_length()
        if count < config.MSG_LOW_BOUND:
            db = boto3.resource('dynamodb', region_name='us-east-1', )
            used_keys = get_used_keys(db)
            new_keys = key_gen(used_keys, num=config.MSG_UP_BOUND - count)
            batch_writer(new_keys)
            send_to_que(new_keys)
        time.sleep(5)
