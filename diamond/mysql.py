"""
    diamond.mysql
    ~~~~~~~~~~~~~

    :copyright: (c) 2015 Jaco Ruit 
    :license: MIT, see LICENSE for more details
"""

import MySQLdb
from database import DatabaseInterface

class MySQLDatabase(DatabaseInterface):
    def __init__(self):
        self.connection = None
        
    def select(self, table, columns, where = None, limit = None, offset = None, groupby_field_name = None, joins = None, print_query = False):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.commit()
        
        cursor = self.connection.cursor()
        query = "SELECT %s" % columns_to_sql(columns)
        query += " FROM `%s`" % table
        if joins != None and len(joins) > 0:      
            for jtable, jclause in joins:
                query += " INNER JOIN `%s` ON %s " % (jtable, parsable_clauses_to_sql(jclause))
        if where != None: query += " WHERE %s" %  parsable_clauses_to_sql(where)
        if limit != None: query += " LIMIT %i" % limit   
        if offset != None: query += " OFFSET %i" % offset
        if groupby_field_name != None: query += " GROUP BY `%s`" % groupby_field_name
       
        if print_query: print(query)
        cursor.execute(query)
        return cursor.fetchall()
    

    def edit(self, table, fieldvalues, where = None, print_query = False):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.commit()
        
        cursor = self.connection.cursor()
        query = "UPDATE `%s`" % table
        query += " SET %s " % ",".join("`%s`='%s'" % fieldvalue for fieldvalue in fieldvalues.items())
        if where != None: query += " WHERE %s" %  parsable_clauses_to_sql(where)
        
        if print_query: print(query)
        try:
            affected = cursor.execute(query)
            self.connection.commit()
            return affected
        except:
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def add(self, table, fieldvalues, print_query = False):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.commit()
        
        cursor = self.connection.cursor()
        query = "INSERT INTO `%s`" % table
        query += " (%s) " % ",".join("`%s`" % fieldname for fieldname in fieldvalues.keys())
        query += " VALUES (%s) " % ",".join("'%s'" % value for value in fieldvalues.values())
        
        if print_query: print(query)
        try:
            affected = cursor.execute(query)
            if affected < 1: return False
            self.connection.commit()
            return cursor.lastrowid
        except:
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def delete(self, table, where = None, print_query = False):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.commit()
        
        cursor = self.connection.cursor()
        query = "DELETE FROM `%s`" % table
        if where != None: query += " WHERE %s" %  parsable_clauses_to_sql(where)
        
        if print_query: print(query)
        try:
            affected = cursor.execute(query)
            if affected < 1: return False
            self.connection.commit()
            return True
        except:
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def connect(self, credentials):
        if self.connection != None: raise Exception("Already connected to MySQL")
        self.connection = MySQLdb.connect(host = credentials["host"],
                                          user = credentials["user"],
                                          passwd = credentials["password"],
                                          db = credentials["database"])
    
    def close(self):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.close()
        del self.connection
        self.connection = None

def columns_to_sql(columns):
    sql = []
    for column in columns:
        type, table, fieldname = column
        string = "`" + table + "`.`" + fieldname + "`"
        if type == "field":
            sql.append(string)
        else:
            sql.append(fn_to_sql(type) + "(" + string + ")")
    return ",".join(sql)
            
def fn_to_sql(fn_name):
    return fn_name.upper()

def fields_to_sql(fields):
    return 

def comparer_to_sql(comparer):
    return comparer.upper()

def parsable_clauses_to_sql(clauses):
    sql = "" 
    for op, clause in clauses:
        if op != None: sql += op.upper()
        if type(clause) == list: 
            sql += " ("
            sql += parsable_clauses_to_sql(clause)
            sql += ") "
        else:
            (table, field), comparer, value = clause
            sql += " `" + table + "`.`" + field + "` "
            sql += comparer_to_sql(comparer)
            if type(value) == tuple:
                otable, ofield = value
                sql += " `" + otable + "`.`" + ofield + "` "
            else: sql += " '" + str(value) + "' "
    return sql