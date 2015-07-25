

class DatabaseInterface(object):
    def connect(self, credentials):
        pass
    
    def close(self):
        pass
    
    def select(self, table, fields, where = None, limit = None, groupby_field_name = None, print_query = False):
        pass
    
    def selectfn(self, table, fn_name, fieldname, where = None, limit = None, groupby_field_name = None, print_query = False):
        pass
    
    def edit(self, table, fieldvalues, where = None, print_query = False):
        pass
    
    def add(self, table, fieldvalues, print_query = False):
        pass
    
    def delete(self, table, where = None, print_query = False):
        pass
