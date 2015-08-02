"""
    diamond.selectables
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2015 Jaco Ruit 
    :license: MIT, see LICENSE for more details
"""


from models import Model

class Selectable(object):
    def __init__(self, columns):
        self.columns = columns
    
    def get_table(self):
        return self.columns[0][1]
    
    def get_columns(self):
        return self.columns
        
    def handle_row(self, row):
        pass
       
class summation(Selectable):
    def __init__(self, field):
        super(summation, self).__init__([("sum", field.table, field.name)])
    
    def handle_row(self, row):
        return int(row[0])

class count(Selectable):
    def __init__(self, obj):
        field = None
        if(issubclass(obj, Model)):
            for mfield in obj.Fields:
                if mfield.primary_key: field = mfield
            if field == None: raise Exception("Model has to have a primary key, or supply a field")
        else: field = obj
        super(count, self).__init__([("count", field.table, field.name)])
    
    def handle_row(self, row):
        return int(row[0])