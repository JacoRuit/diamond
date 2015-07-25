from queries import SelectQuery, EditQuery, AddQuery, RemoveQuery, SelectFunction
from models import Model
import types

class Diamond(object):
    def __init__(self, database):
        self.db = database
        self.debug = False
    
    def select(self, obj):
        if isinstance(obj,  type) and issubclass(obj, Model):
            return SelectQuery(self, "models", obj)
        if issubclass(type(obj), SelectFunction):
            return SelectQuery(self, "fn", obj)
        raise Exception("Invalid object passed to select")
    
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




    