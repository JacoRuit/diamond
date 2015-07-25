from diamond.models import build_model_instance, Model
from diamond.where import parsable

class SelectFunction(object):
    def __init__(self, name, field):
        self.name = name
        self.field = field
        
    def handle_rows(self, rows):
        return rows

class summation(SelectFunction):
    def __init__(self, field):
        super(summation, self).__init__("sum", field)
    
    def handle_rows(self, rows):
        if len(rows) < 1: return None
        return int(rows[0][0])

class count(SelectFunction):
    def __init__(self, obj):
        field = None
        if(issubclass(obj, Model)):
            for mfield in obj.Fields:
                if mfield.primary_key: field = mfield
            if field == None: raise Exception("Model has to have a primary key, or supply a field")
        else: field = obj
        super(count, self).__init__("count", field)
    
    def handle_rows(self, rows):
        if len(rows) < 1: return None
        return int(rows[0][0])

class Query(object):
    def __init__(self, diamond, modeltype):
        self.diamond = diamond
        self.modeltype = modeltype
    
    def execute(self):
        pass
    
class SelectQuery(Query):
    def __init__(self, diamond, type, obj):
        super(SelectQuery, self).__init__(diamond, None if type == "fn" else obj)
        self.selectfunction = None if type == "models" else obj
        self.__type = type
        self.__where = None
        self.__groupby_field_name = None
        self.__limit = None

    def where(self, node):
        if self.__where != None: raise Exception("Where already set!")
        self.__where = parsable(node)
        return self
    
    def groupby(self, groupby_field):
        if self.__groupby_field_name != None: raise Exception("Group by field already set")
        self.__groupby_field_name = groupby_field.name
        return self
    
    def limit(self, limit):
        if self.__limit != None: raise Exception("Limit already set")
        self.__limit = limit
        return self
    
    def __execute_models(self):
        models = []
        rows = self.diamond.db.select(self.modeltype.Table, 
                                      [field.name for field in self.modeltype.Fields],
                                      where = self.__where,
                                      groupby_field_name = self.__groupby_field_name,
                                      limit = self.__limit,
                                      print_query = self.diamond.debug)
        for row in rows:
            models.append(build_model_instance(self.modeltype, row))
        if self.__limit == 1:
            return models[0] if len(models) > 0 else None
        return models
    
    def __execute_fn(self):
        rows = self.diamond.db.selectfn(self.selectfunction.field.table,
                                        self.selectfunction.name, 
                                        self.selectfunction.field.name,
                                        where = self.__where,
                                        groupby_field_name = self.__groupby_field_name,
                                        limit = self.__limit,
                                        print_query = self.diamond.debug)
        return self.selectfunction.handle_rows(rows)
    
    def execute(self):
        if self.__type == "fn": return self.__execute_fn()
        if self.__type == "models": return self.__execute_models()
        raise Exception("Invalid SelectQuery type")

    
class EditQuery(Query):
    def __init__(self, diamond, modeltype):
        super(EditQuery, self).__init__(diamond, modeltype)
        self.__where = None
        self.__set = {}

    def where(self, node):
        if self.__where != None: raise Exception("Where already set!")
        self.__where = parsable(node)
        return self
    
    def set(self, val1, val2 = None):
        if type(val1) == dict:
            self.__set.update(val1)
        else: self.__set[val1] = val2
        return self
    
    def execute(self):
        if len(self.__set) < 1: return True
        fieldnamevalues = {}
        for (field, value) in self.__set.items():
            if not field.is_acceptable_value(value):
                raise Exception("Invalid type for %s" % field.name)
            fieldnamevalues[field.name] = value
        return self.diamond.db.edit(self.modeltype.Table,
                                    fieldnamevalues,
                                    where = self.__where,
                                    print_query = self.diamond.debug)

class AddQuery(Query):
    def __init__(self, diamond, modeltype):
        super(AddQuery, self).__init__(diamond, modeltype)
        self.__set = {}
    
    def set(self, val1, val2 = None):
        if type(val1) == dict:
            self.__set.update(val1)
        else: self.__set[val1] = val2
        return self
    
    def execute(self):
        fieldnamevalues = {}
        for (field, value) in self.__set.items():
            if not field.is_acceptable_value(value):
                raise Exception("Invalid type for %s" % field.name)
            fieldnamevalues[field.name] = value
        return self.diamond.db.add(self.modeltype.Table,
                                   fieldnamevalues,
                                   print_query = self.diamond.debug)
            
class RemoveQuery(Query):
    def __init__(self, diamond, modeltype):
        super(RemoveQuery, self).__init__(diamond, modeltype)
        self.__where = None   
    
    def where(self, node):
        if self.__where != None: raise Exception("Where already set!")
        self.__where = parsable(node)
        return self
    
    def execute(self):
        return self.diamond.db.delete(self.modeltype.Table,
                                      where = self.__where,
                                      print_query = self.diamond.debug)     