class Node(object):
    def __init__(self, inner = None, condition = None):
        self.inner = inner
        self.condition = condition
        self.children = []
        
    def __and__(self, other):
        self.children.append(("and", other))
        return self
        
    def __or__(self, other):
        self.children.append(("or", other))
        return self

def parsable(node):
    parsable_clause = []
    nodes = node.children
    nodes.insert(0, (None, node))
    for op, node in nodes:
        if node.inner != None: parsable_clause.append((op, parsable(node.inner)))
        else: parsable_clause.append((op, node.condition))
    return parsable_clause

def clause(node):
    return Node(inner = node)

def condition(field, comparer, value):
    return Node(condition = (field, comparer, value))