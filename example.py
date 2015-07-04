import random

from gevent import monkey
monkey.patch_all()

import beanstalkc
from flask import Flask


class Beanstalk(beanstalkc.Connection):

    def __init__(self, app=None):
        if app:
            self.init_app(app)
            self.app = app

    def init_app(self, app):
        conn_kwargs = {}
        for n in ('host', 'port', 'parse_yaml', 'conn_timeout'):
            v = app.config.get('BEANSTALK_' + n.upper())
            if v:
                conn_kwargs[n] = v
        super(Beanstalk, self).__init__(**conn_kwargs)

app = Flask(__name__)
beanstalk = Beanstalk(app)


@app.route('/')
def index():
    secs = random.randint(0, 10)
    beanstalk.put(str(secs))
    return "placed job that sleeps for %d seconds" % secs


if __name__ == '__main__':
    app.run(debug=True)
