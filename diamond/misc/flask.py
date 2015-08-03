"""
    diamond.misc.flask
    ~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2015 Jaco Ruit 
    :license: MIT, see LICENSE for more details
"""

from __future__ import absolute_import
try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack
from diamond import Diamond


class FlaskyDiamond(object):
    def __init__(self, credentials, databaseinterface):
        self.credentials = credentials
        self.databaseinterface = databaseinterface
        
    def init_app(self, app):
        if hasattr(app, "teardown_appcontext"):
            app.teardown_appcontext(self.teardown)
        else:
            app.teardown_request(self.teardown)

    def teardown(self, exception):
        ctx = stack.top
        if hasattr(ctx, "diamond"):
            ctx.diamond.close()
    
    @property
    def diamond(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, "diamond"):
                ctx.diamond = Diamond(self.databaseinterface())
                ctx.diamond.connect(self.credentials)
            return ctx.diamond