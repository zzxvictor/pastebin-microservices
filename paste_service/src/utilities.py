import json

import boto3
import base64
import pika
import redis
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging
import config
from typing import Any, Union


class Encryption:
    @classmethod
    def _get_encryption_key(cls, key: str) -> bytes:
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), salt=config.SALT,
                         length=32, iterations=100)
        encryption_key = base64.urlsafe_b64encode(kdf.derive(key.encode()))
        return encryption_key

    @classmethod
    def encrypt(cls, key: str, content: str) -> bytes:
        if key == '':
            logging.log(level=config.LOGGING_LEVEL, msg="no encryption")
            return content.encode()
        encryption_key = cls._get_encryption_key(key)
        f = Fernet(encryption_key)
        logging.log(level=config.LOGGING_LEVEL, msg="msg encrypted")
        return f.encrypt(content.encode())

    @classmethod
    def decrypt(cls, key: str, content: bytes) -> str:
        if key == '':
            logging.log(level=config.LOGGING_LEVEL, msg="no encryption")
            return content.decode()
        encryption_key = cls._get_encryption_key(key)
        f = Fernet(encryption_key)
        logging.log(level=config.LOGGING_LEVEL, msg="msg encrypted")
        return f.decrypt(content).decode()


class Dynamo:
    @classmethod
    def get_item(cls, client, pk: str) -> Any:
        return None

    @classmethod
    def put_item(cls, client, item: dict) -> False:
        logging.log(config.LOGGING_LEVEL, item)
        try:
            res = client.put_item(
                TableName=config.TABLE_NAME,
                Item=item
            )
            return True
        except Exception as e:
            logging.log(config.LOGGING_LEVEL, e)
            return False


class Redis:
    @classmethod
    def get_item(cls, client: Any, pk: str) -> Any:
        return None

    @classmethod
    def put_item(cls, client: Any, item: dict) -> Any:
        logging.log(config.LOGGING_LEVEL, 'Redis: {}'.format(item))
        key = item['uid']
        content = item['content']
        expire_date = item['expire_on']
        try:
            client.hmset(key, item)
            client.expireat(key, int(expire_date))
            return True
        except Exception as e:
            logging.log(config.LOGGING_LEVEL, 'redis error {}'.format(e))
            return False


class MessageQue:
    @classmethod
    def get_message(cls, client) -> Union[dict, None]:
        channel = client.channel()
        method_frame, header_frame, body = channel.basic_get(config.QUEUE)
        if method_frame:
            msg = body.decode()
            logging.log(config.LOGGING_LEVEL, 'message retrieved: ' + msg)
            msg = json.loads(msg)
            return msg
        else:
            return None
