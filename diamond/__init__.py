"""
    diamond
    ~~~~~~~

    :copyright: (c) 2015 Jaco Ruit 
    :license: MIT, see LICENSE for more details
"""

from queries import SelectQuery, EditQuery, AddQuery, RemoveQuery

class Diamond(object):
    def __init__(self, database):
        self.db = database
        self.debug = False
    
    def select(self, *args):
        return SelectQuery(self, list(args))
    
    def edit(self, modeltype):
        return EditQuery(self, modeltype)
    
    def add(self, modeltype):
        return AddQuery(self, modeltype)
    
    def remove(self, modeltype):
        return RemoveQuery(self, modeltype)
    
    def connect(self, credentials):
        self.db.connect(credentials)
    
    def close(self):
        self.db.close()




    