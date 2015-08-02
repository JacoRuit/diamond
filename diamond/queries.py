"""
    diamond.queries
    ~~~~~~~~~~~~~~~

    :copyright: (c) 2015 Jaco Ruit 
    :license: MIT, see LICENSE for more details
"""


from diamond.clauses import parsable

class Query(object):
    def __init__(self, diamond):
        self.diamond = diamond
    
    def execute(self):
        pass
    
class SelectQuery(Query):
    def __init__(self, diamond, selectables):
        super(SelectQuery, self).__init__(diamond)
        self.selectables = selectables
        if len(self.selectables) < 1: 
            raise Exception("You have to provide at least one selectable object")
        self.table = self.selectables[0].get_table()
        for selectable in self.selectables:
            if selectable.get_table() != self.table:
                raise Exception("Can't select data from different tables. Use joins!")
        self.__where = None
        self.__groupby_field_name = None
        self.__limit = None
        self.__offset = None
        self.__joins = []

    def where(self, node):
        if self.__where != None: raise Exception("Where already set!")
        self.__where = parsable(node)
        return self
    
    def sortby(self):
        pass #TODO implement
    
    def groupby(self, groupby_field):
        if self.__groupby_field_name != None: raise Exception("Group by field already set")
        self.__groupby_field_name = groupby_field.name
        return self
    
    def limit(self, limit):
        if self.__limit != None: raise Exception("Limit already set")
        self.__limit = limit
        return self
    
    def join(self, selectable, on_clause):
        self.__joins.append((selectable, parsable(on_clause)))
        return self
    
    def offset(self, offset):
        self.__offset = offset
    
    def lazyiter(self):
        offset = 0 
        while True:
            rows = self.execute(limit = 1, offset = offset)
            if len(rows) < 1: break
            yield rows[0]
            offset += 1
    
    def execute(self, **overwrite_parameters):
        columns = []
        selectable_column_indices = []
        
        selectables = self.selectables
        joins = []
        
        for jselectable,jclause in self.__joins:
            selectables.append(jselectable)
            joins.append((jselectable.get_table(), jclause))
        
        p = 0
        for selectable in selectables:
            selectable_columns = selectable.get_columns()
            columns += selectable_columns
            np = p + len(selectable_columns)
            selectable_column_indices.append((p, np))
            p = np
            
        limit = self.__limit if not "limit" in overwrite_parameters.keys() else overwrite_parameters["limit"]
        offset = self.__offset if not "offset" in overwrite_parameters.keys() else overwrite_parameters["offset"]
        
        rows = self.diamond.db.select(self.table, 
                                      columns,
                                      where = self.__where,
                                      groupby_field_name = self.__groupby_field_name,
                                      limit = limit,
                                      offset = offset,
                                      joins = joins,
                                      print_query = self.diamond.debug)
        
        nrows = []
        for row in rows:
            nrow = []
            for i in range(0, len(selectables)):
                idx1, idx2 = selectable_column_indices[i]
                nrow.append(selectables[i].handle_row(row[idx1:idx2]))
            if len(selectables) < 2: nrows.append(nrow[0]) 
            else: nrows.append(nrow)
        return nrows

    
class EditQuery(Query):
    def __init__(self, diamond, modeltype):
        super(EditQuery, self).__init__(diamond)
        self.modeltype = modeltype
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
        super(AddQuery, self).__init__(diamond)
        self.modeltype = modeltype
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
        super(RemoveQuery, self).__init__(diamond)
        self.modeltype = modeltype
        self.__where = None   
    
    def where(self, node):
        if self.__where != None: raise Exception("Where already set!")
        self.__where = parsable(node)
        return self
    
    def execute(self):
        return self.diamond.db.delete(self.modeltype.Table,
                                      where = self.__where,
                                      print_query = self.diamond.debug)     