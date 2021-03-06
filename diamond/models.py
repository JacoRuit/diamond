"""
    diamond.models
    ~~~~~~~~~~~~~~

    :copyright: (c) 2015 Jaco Ruit 
    :license: MIT, see LICENSE for more details
"""


import new
from diamond.clauses import condition

class Field(object):
    def __init__(self, type, primary_key = False):
        self.name = None #this is set after create_model(..)
        self.table = None #this is set after create_model(..)
        self.type = type
        self.primary_key = primary_key
    
    def is_acceptable_value(self, value):
        return type(value) == self.type
    
    def __condition(self, comparer, value):
        if not isinstance(value, Field) and not self.is_acceptable_value(value):
            raise Exception("Invalid type for %s" % self.name)
        return condition(self, comparer, value)
    
    def equals(self, value):
        return self.__condition("=", value)
    
    def nequals(self, value):
        return self.__condition("!=", value)
    
    def greaterthan(self, value):
        return self.__condition(">", value)
    
    def lessthan(self, value):
        return self.__condition("<", value)
    
    def greaterequal(self, value):
        return self.__condition(">=", value)
    
    def lessequal(self, value):
        return self.__condition("<=", value)
    
    def like(self, value):
        return self.__condition("like", value)
    
    def notlike(self, value):
        return self.__condition("not like", value)
    
    def get_table(self):
        return self.table
    
    def get_columns(self):
        return [("field", self.table, self.name)]
    
    def handle_row(self, row):
        return self.type(row[0])
    
    
class Collection(object):
    def __init__(self, name, modeltype, clause, orderby = None):
        self.modeltype = modeltype
        self.clause = clause
        self.name = name
        self.orderby = None
    

class Model(object):
    Table = ""
    Fields = []
    Collections = []
    
    def save(self): raise Exception("Call create_model first and make sure your model has a primary key")
    
    @staticmethod
    def get(pk): raise Exception("Call create_model first and make sure your model has a primary key")
    
    @staticmethod
    def edit(pk, **kwargs): raise Exception("Call create_model first and make sure your model has a primary key")        
    
def get_field(modeltype, fieldname):
    for  field in modeltype.Fields:
        if field.name == fieldname: return field
    return None
    
def create_model(diamond, modeltype):   
    pk_field = None
    modeltype.Fields = []
    modeltype.Collections = []
    for name in dir(modeltype):
        value = getattr(modeltype, name)
        if type(value) == Field:
            if value.primary_key: pk_field = value
            value.name = name
            value.table = modeltype.Table
            modeltype.Fields.append(value)
        elif isinstance(value, Collection):
            modeltype.Collections.append(value)

    if pk_field == None: 
        raise Exception("Diamond doesn't allow models without a primary key")
    
    @staticmethod
    def get_table():
        return modeltype.Table 
    modeltype.get_table = get_table
       
    @staticmethod
    def get_columns():
        return [("field", modeltype.Table, field.name) for field in modeltype.Fields]
    modeltype.get_columns = get_columns
        
    @staticmethod
    def handle_row(row):
        return build_model_instance(modeltype, row)
    modeltype.handle_row = handle_row
    
    @staticmethod
    def get(pk):
        rows = diamond.select(modeltype).where(pk_field.equals(pk)).limit(1).execute()
        if len(rows) < 1: return None
        return rows[0]
    modeltype.get = get
    
    @staticmethod
    def edit(pk, **kwargs):
        fieldvalues = {}
        for (fieldname, value) in kwargs.items():
            fieldvalues[get_field(modeltype, fieldname)] = value
        #if pk_value == None: raise Exception("Primary key value not given")
        affected = diamond.edit(modeltype).set(fieldvalues).where(pk_field.equals(pk)).execute()
        if not affected or affected < 1: return False
        return True
    modeltype.edit = edit
       
    def add_collections(model): 
        for i in range(0, len(modeltype.Collections)):
            setattr(model, "__collection_%i" % i, None)
        
        for i in range(0, len(modeltype.Collections)):
            collection = modeltype.Collections[i]
            def retriever(self):
                pk_value = getattr(self, pk_field.name)
                value = getattr(self, "__collection_%i" % i)
                if value == None: 
                    print "selecting"
                    query = diamond.select(collection.modeltype).join(modeltype, pk_field.equals(pk_value) & collection.clause)
                    if collection.orderby != None: query.orderby(collection.orderby)
                    rows = query.execute()
                    value = [row[0] for row in rows]
                    setattr(model, "__collection_%i" % i, value) 
                return value
            setattr(model, collection.name, new.instancemethod(retriever, model, type(model)))

    def __init__(self, **kwargs):
        if pk_field.name in kwargs.keys():
            raise Exception("You can't provide a primary key")
        for field in self.Fields:
            if not field.name in kwargs.keys(): continue
            value = kwargs[field.name]
            if not field.is_acceptable_value(value):
                raise Exception("Invalid type for %s" % field.name)
            setattr(self, field.name, value)
        add_collections(self)
    modeltype.__init__ = __init__
    
    def save(self):
        new = not has_field_set(self, pk_field.name)
        pk_value = None
        fieldvalues = {}
        for field in self.Fields:
            if not has_field_set(self, field.name): continue
            value = getattr(self, field.name)
            if field.primary_key: 
                pk_value = value
                continue
            fieldvalues[field] = value
        if new:
            pk_new = diamond.add(self).set(fieldvalues).execute()
            if pk_new != False: 
                setattr(self, pk_field.name, pk_field.type(pk_new))
                add_collections(self)
            return pk_new != False
        affected = diamond.edit(self).set(fieldvalues).where(pk_field.equals(pk_value)).execute()
        return False if not affected or affected < 1 else True
    modeltype.save = save

def has_field_set(model, field_name):
    return hasattr(model, field_name) and type(getattr(model, field_name)) != Field

def build_model_instance(modeltype, row):
    model = modeltype()
    i = 0
    for field in modeltype.Fields:
        setattr(model, field.name, field.type(row[i]))
        i += 1
    return model
