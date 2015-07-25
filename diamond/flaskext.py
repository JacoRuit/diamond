from __future__ import absolute_import
from flask import _request_ctx_stack
from diamond import Diamond


#Forked from https://github.com/cyberdelia/flask-mysql/blob/master/flaskext/mysql.py
#TODO: give credits
class DiamondFlask(object):
    def __init__(self, app=None, credentials, databaseinterface):
        self.credentials = credentials
        self.databaseinterface = databaseinterface

    def init_app(self, app):
        self.app.teardown_request(self.teardown_request)
        self.app.before_request(self.before_request)

    def connect(self):
        self.diamond = Diamond(self.databaseinterface)
        self.diamond.connect(self.credentials)

    def before_request(self):
        ctx = _request_ctx_stack.top
        ctx.diamond = self.connect()

    def teardown_request(self, exception):
        ctx = _request_ctx_stack.top
        if hasattr(ctx, "diamond"):
            ctx.diamond.close()

    def get_diamond(self):
        ctx = _request_ctx_stack.top
        if ctx is not None:
            return ctx.diamond