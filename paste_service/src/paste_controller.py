import logging
from typing import Any, Union
from utilities import Encryption, Dynamo, MessageQue, Redis
import datetime
import config
from decimal import Decimal, ROUND_UP


class UriNotFoundError(Exception):
    """Raise when item is not found in DB"""
    def __init__(self, uid, message="URI is not valid"):
        self.uid = uid
        self.message = message
        super().__init__(self.message)

    def format_msg(self):
        return "Issue with resource: {}, error message: {}".format(self.uid, self.message)


class InvalidUriError(Exception):
    """Raise when uri is invalid"""
    def __init__(self, uid, message="URI is not valid"):
        self.uid = uid
        self.message = message
        super().__init__(self.message)

    def format_msg(self):
        return "Issue with resource: {}, error message: {}".format(self.uid, self.message)


class ContentTooBigError(Exception):
    """Raise when paste content is too big"""
    def __init__(self, uid, message="content too big"):
        self.uid = uid
        self.message = message
        super().__init__(self.message)

    def format_msg(self):
        return "Issue with paste content, error message: {}".format(self.uid, self.message)


class UploadController:
    @classmethod
    def handle_request(cls, key: str,
                       content: str,
                       db_client: Any,
                       redis_client: Any,
                       mq_client: Any) -> str:
        if len(content) < config.MAX_PASTE_SIZE:
            raise ContentTooBigError('size got: {} > max size {}'.format(len(content), config.MAX_PASTE_SIZE))

        data_to_save = Encryption.encrypt(key, content)
        life_span = datetime.timedelta(days=config.LIFE_SPAN)
        valid_until = (datetime.datetime.utcnow() + life_span).timestamp()
        counter = 0
        uid = ''

        while counter < config.QUEUE_RETRY:
            msg = MessageQue.get_message(mq_client)
            assert msg is not None, "no message failure"
            uid, key_expire = msg['uid'], msg['expire_on']
            if key_expire > valid_until:
                break
            else:
                logging.warning('stale key retrieved, this should rarely happen in practice')
                counter += 1

        assert counter < config.QUEUE_RETRY, 'maximum retry exceed!'
        assert uid != '', 'uid invalid'

        valid_util = Decimal(valid_until).quantize(Decimal('1.'), rounding=ROUND_UP)
        res = Dynamo.put_item(db_client, item={'uid': {'S': uid},
                                               'content': {'B': data_to_save},
                                               'expire_on': {'N': str(valid_util)}})

        assert res == True, 'db insertion failed'

        res = Redis.put_item(redis_client, item={'uid': uid,
                                                 'content': data_to_save,
                                                 'expire_on': float(valid_util)})

        if not res:
            logging.warning('Redis insertion failed, this should not happen!')
        return uid


class RetrieveController:
    @classmethod
    def handle_request(cls, key: str,
                       password: str,
                       redis_client: Any,
                       db_client: Any) -> Union[str, None]:
        if len(key) != config.KEY_SIZE:
            raise InvalidUriError(key, "key must be have length {}".format(config.KEY_SIZE))

        data = Redis.get_item(redis_client, key)
        if len(data) != 0 and False:
            logging.log(config.LOGGING_LEVEL, 'data in cache {}'.format(data))
            content = {'uid': data[b'uid'], 'expire_on': data[b'expire_on'].decode()}
        else:
            data = Dynamo.get_item(db_client, key)
            if data is not None:
                logging.log(config.LOGGING_LEVEL, 'data in db {}'.format(data))
                content = {key: list(val.values())[0] for key, val in data.items()}
            else:
                raise UriNotFoundError(key, message="Not a valid resource id or expired")

        if datetime.datetime.utcnow().timestamp() < float(content['expire_on']):
            return Encryption.decrypt(password, content['content'])
        else:
            raise UriNotFoundError(key, message="Not a valid resource id or expired")
