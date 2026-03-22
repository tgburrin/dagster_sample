import json
import re
import datetime
import uuid
import decimal


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        str_types = (uuid.UUID, decimal.Decimal, datetime.timedelta)
        dt_types = (datetime.datetime, datetime.date, datetime.time)

        if isinstance(o, str_types):
            return str(o)
        elif isinstance(o, dt_types):
            return o.isoformat()
        elif isinstance(o, memoryview):
            return o.tobytes().hex()
        elif isinstance(o, bytes):
            return o.decode()
        else:
            return json.JSONEncoder.default(self, o)


class CustomJsonDecoder(json.JSONDecoder):
    """
    This custom JSON decoder, built for Python 3.x, removes illegal characters during the decoding process.
    """
    illegal_unicode = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + \
        u'|' + u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
               (chr(0xd800), chr(0xdbff), chr(0xdc00), chr(0xdfff),
                chr(0xd800), chr(0xdbff), chr(0xdc00), chr(0xdfff),
                chr(0xd800), chr(0xdbff), chr(0xdc00), chr(0xdfff))

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.custom_hook, *args, **kwargs)

    def custom_hook(self, obj):
        removed_keys = []
        for k, v in obj.items():
            if isinstance(v, str):
                obj[k] = re.sub(self.illegal_unicode, '', v)
            elif v is None:
                removed_keys.append(k)

        for k in removed_keys:
            del obj[k]

        return obj