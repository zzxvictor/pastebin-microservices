import json
import base64
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging
import config
from typing import Any, Union
import random


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
        try:
            f = Fernet(encryption_key)
            logging.log(level=config.LOGGING_LEVEL, msg="msg encrypted")
            data = f.decrypt(content).decode()
        except InvalidToken:
            data = cls._gibberish(key).decode()
        return data

    @classmethod
    def _gibberish(cls, key: str) -> bytes:
        message = str(hash(key))
        password = ''.join([str(random.randint(0, 9)) for _ in range(10)])
        return cls.encrypt(password, message)


class Dynamo:
    @classmethod
    def get_item(cls, client, pk: str) -> Any:
        logging.log(config.LOGGING_LEVEL, 'dynamo get item {}'.format(pk))
        try:
            res = client.get_item(
                TableName=config.TABLE_NAME,
                Key={'uid': {'S': pk}}
            )
            return res.get('Item', None)
        except Exception as e:
            logging.log(config.LOGGING_LEVEL, 'dynamo get item error - {}'.format(e))
            return None

    @classmethod
    def put_item(cls, client, item: dict) -> False:
        logging.log(config.LOGGING_LEVEL, 'dynamo put item {}'.format(item))
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
        logging.log(config.LOGGING_LEVEL, 'Redis - get item: {}'.format(pk))
        data = client.hgetall(pk)
        return data

    @classmethod
    def put_item(cls, client: Any, item: dict) -> Any:
        logging.log(config.LOGGING_LEVEL, 'Redis put item: {}'.format(item))
        key = item['uid']
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

