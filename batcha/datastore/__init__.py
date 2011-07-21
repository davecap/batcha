from tables import *

# import numpy
# import os
# import inspect

# _tables = []

# def create(filename, module=self, title="datastore"):
#     """ 
#     Create the pytables data file at the given filename. It will not run if the file already exists.
#     For each class defined in this file which inherits IsDescription, the appropriate table will be created.
#     If the class name has an underscore, the first part will be used as the group name.
#     For example:
#         Analysis_Dihedrals(IsDescription):
#     will be a table 'dihedrals' in the 'analysis' group.
#     """
#     global _tables
#     
#     if os.path.exists(filename):
#         print "Will not create datastore file at %s, file exists!" % filename
#         return False
#         
#     for name in dir(module):
#        obj = getattr(module, name)
#        if inspect.isclass(obj) and obj.__base__.__name__ == 'IsDescription':
#            _tables.append(obj)
#     
#     h5file = openFile(filename, mode="w", title=title)
#     group = h5file.createGroup("/", 'detector', 'Detector information')
#     table = h5file.createTable(group, 'readout', Particle, "Readout example")

class Column(object):
    _data = None # data to be written
    _dirty = False
    path = None
    name = None
    format = None
    
    def __init__(self, path, name, format):
        # print "Creating Column(%s)" % name
        self.path = path
        self.name = name
        self.format = format
        self._data = []
        self._dirty = False
    
    def load(self, data):
        # print "Loading data:"
        if type(data) is list:
            self._data += data
        else:
            self._data.append(data)
        self._dirty = True
    
    def dirty_row_count(self):
        return len(self._data)
    
    def next_dirty_row(self):
        if not self._dirty:
            return False
        
        row = self._data.pop(0)
        if len(self._data) == 0:
            self._dirty = False
        return row

class Array(Column):
    """ Array inherits from Column because it's basically a table with a single column """
    
    def __init__(self, h5f, full_path, format=ObjectAtom()):
        self._h5f = h5f
        self.full_path = full_path
        (path, name) = split_path(self.full_path)
        super(Array, self).__init__(path, name, format)
    
    def setup(self):
        print "Setting up array at: %s" % (self.full_path)
        split_path = self.full_path.split('/')
        # traverse the path
        node = self._h5f.getNode('/')
        for n in split_path[1:]:
            parent = node._v_pathname
            path = (parent+'/'+n).replace('//','/')
            try:
                # try to get the node
                node = self._h5f.getNode(path)
            except NoSuchNodeError:
                if path == self.full_path:
                    # we are at the array but it doesn't exist yet so create it
                    # node = self._h5f.createEArray(node, self.name, self.format, (len(self._data[0]), ), expectedrows=25000)
                    node = self._h5f.createVLArray(node, self.name, self.format, filters=Filters(1))
                    print "Created array: %s" % path
                else:
                    # we are at a group that doesn't exist yet
                    node = self._h5f.createGroup(node, n)
                    print "Created group: %s" % path
        
        # node should now be the array node
        self._node = node
        return self._node
        
    def write(self):
        if self.dirty_row_count() == 0:
            print "Array %s has no rows to write, skipping it!" % self.full_path
            return False
        
        # first make sure the array is setup
        self.setup()
        
        print "Appending %d rows..." % self.dirty_row_count()
        while self._dirty:
            self._node.append(self.next_dirty_row())
        print " Done."

class Table(object):
    """ Dataset stored with PyTables
    """
    
    # PyTables table group, name and description
    
    _h5f = None # HDF5 file object (not initialized by this object)
    _node = None # the initialized table
    
    path = None
    name = None  # name of the table to be created/used
    
    _columns = {}
    
    def __init__(self, h5f, path):
        # print "Creating Table(%s)" % path
        self._h5f = h5f
        self._node = None
        self._columns = {}
        self.path = path
        self.name = self.path.split('/')[-1]
    
    def _description(self):
        desc = {}
        for col in self._columns.values():
            desc[col.name] = col.format
        return desc
    
    def column(self, name, format):
        # print "Adding column %s to table %s" % (name, self.path)
        if name not in self._columns:
            self._columns[name] = Column(self.path, name, format)
        return self._columns[name]
    
    def setup(self):
        print "Setting up table at: %s" % self.path
        split_path = self.path.split('/')
        # traverse the path
        node = self._h5f.getNode('/')
        for n in split_path[1:]:
            parent = node._v_pathname
            path = (parent+'/'+n).replace('//','/')
            try:
                # try to get the node
                node = self._h5f.getNode(path)
            except NoSuchNodeError:
                if path == self.path:
                    # we are at the table but it doesn't exist yet so create it
                    node = self._h5f.createTable(node, n, self._description(), expectedrows=25000)
                    print "Created table: %s" % path
                else:
                    # we are at a group that doesn't exist yet
                    node = self._h5f.createGroup(node, n)
                    print "Created group: %s" % path
            else:
                # if we find the node but the descriptions differ (column(s) added or removed)
                if path == self.path and set(node.description._v_colObjects.keys()) != set(self._description().keys()):
                    print "Column(s) modified for table: %s" % self.path
                    print set(node.description._v_colObjects.keys()) ^ set(self._description().keys())
                    copy_node = self._h5f.createTable(node._v_parent, self.name+'_COPY', self._description(), expectedrows=25000)
                    node.attrs._f_copy(copy_node)
                    for i in xrange(node.nrows):
                        copy_node.row.append()
                    copy_node.flush()
                    # copy the data from the old table
                    for col in node.description._v_colObjects:
                        # only if the col is in the new table
                        if col in self._description():
                            getattr(copy_node.cols, col)[:] = getattr(node.cols, col)[:]
                    copy_node.flush()
                    node.remove()
                    copy_node.move(parent, self.name)
                    node = copy_node
                    print "Table %s updated with new column(s)." % self.path
        
        # node should now be the table object
        self._node = node
        return self._node
        
    def write(self):
        num_rows = [ col.dirty_row_count() for col in self._columns.values() ]
        num_rows = set(num_rows)
        if len(num_rows) > 1:
            raise Exception('Inconsistent number of rows to write: %s' % num_rows)
        num_rows = list(num_rows)[0]
        
        # first make sure the table is setup
        self.setup()
        
        print "Appending %d rows..." % num_rows
        row = self._node.row
        for i in range(num_rows):
            if i % 10 == 0:
                print '.',
            for col in self._columns.values():
                row[col.name] = col.next_dirty_row()
            row.append()
        print " Done."
        self._node.flush()
    
