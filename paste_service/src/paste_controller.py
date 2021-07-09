import logging
from typing import Any
from utilities import Encryption, Dynamo, MessageQue, Redis
import datetime
import config
from decimal import Decimal, ROUND_UP


class UploadController:
    @classmethod
    def handle_request(cls, key: str,
                       content: str,
                       db_client: Any,
                       redis_client: Any,
                       mq_client: Any) -> str:
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


