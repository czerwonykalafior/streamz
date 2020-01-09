import requests
from lxml import etree
from streamz import Stream
import logging

logger = logging.getLogger(__name__)


class Fetch:
    def __init__(self, url, method='GET', headers=None, body=None):
        self.url = url
        self.method = method
        self.headers = headers
        self.body = body

    def __repr__(self):
        return str(self.url)


class Payload:
    def __init__(self, request_made: Fetch = None, response=None, precollected=None, all_the_rest=None):
        self.request_made = request_made
        self.precollected = precollected
        self.response = response
        self.all_the_rest = all_the_rest
        self.html = etree.HTML('')

    def initialize(self, r):
        if type(r) == Fetch:
            self.request_made = r
            self.response = requests.get(r.url)
            self.html = etree.HTML(self.response.text)
        elif type(r) == Data:
            self.precollected = r
        else:
            self.all_the_rest = r

    def __str__(self):
        return str({'request_made': self.request_made,
                    'precollected': self.precollected})


class Data(dict):
    pass


def build_payload(x):
    payload = Payload()
    if isinstance(x, (list, tuple, set)):
        for r in x:
            payload.initialize(r)
    else:
        payload.initialize(x)
    return payload


@Stream.register_api()
class Fly(Stream):

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        payload = build_payload(x)
        try:
            results = self.func(payload, *self.args, **self.kwargs)

        except Exception as e:
            logger.exception(f'ARGS: {payload}')
            logger.exception(e)
            raise
        else:
            return self._emit(results)
