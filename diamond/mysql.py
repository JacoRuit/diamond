'''
Created on 24 jul. 2015

@author: Jaco
'''

import MySQLdb
from database import DatabaseInterface

class MySQLDatabase(DatabaseInterface):
    def __init__(self):
        self.connection = None
        
    def select(self, table, fields, where = None, limit = None, groupby_field_name = None, print_query = False):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.commit()
        cursor = self.connection.cursor()
        query = "SELECT %s" % ",".join("`" + field + "`" for field in fields)
        query += " FROM `%s`" % table
        if where != None: query += " WHERE %s" %  parsable_clauses_to_sql(where)
        if limit != None: query += " LIMIT %i" % limit
        if groupby_field_name != None: query += " GROUP BY `%s`" % groupby_field_name
        if print_query: print(query)
        cursor.execute(query)
        return cursor.fetchall()
    
    def selectfn(self, table, fn_name, fieldname, where = None, limit = None, groupby_field_name = None, print_query = False):
        if self.connection == None: raise Exception("Not connected to MySQL")
        self.connection.commit()
        cursor = self.connection.cursor()
        query = "SELECT %s" % fn_to_sql(fn_name, fieldname)
        query += " FROM `%s`" % table
        if where != None: query += " WHERE %s" %  parsable_clauses_to_sql(where)
        if limit != None: query += " LIMIT %i" % limit
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

def fn_to_sql(fn_name, fieldname):
    return "%s(`%s`)" % (fn_name.upper(), fieldname)

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
            field, comparer, value = clause
            sql += " `" + field + "` "
            sql += comparer_to_sql(comparer)
            sql += " '" + str(value) + "' "
    return sql