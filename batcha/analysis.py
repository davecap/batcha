from MDAnalysis import *
from MDAnalysis import collection, SelectionError

import tables
import numpy
import os

from batcha.utils import split_path
from batcha.datastore import Table, Array

class Analysis():
    def __init__(self, filename, title="datastore", readonly=True):
        self._filename = filename
        self._title = title
        self._readonly = readonly
        self._h5f = self.open_or_create()
        self.reset()

    def reset(self):
        # nodes holds all Table and Array objects which take care of table creation and writing
        self._nodes = {}
        # the following dicts store the actual analyses which are processed
        self._sequential = {}
        self._timeseries = {}

    def get_or_create_array(self, path):
        if path not in self._nodes:
            self._nodes[path] = Array(self._h5f, path)
        return self._nodes[path]
            
    def get_or_create_column(self, path, format):
        (table_path, col_name) = split_path(path)
        if table_path not in self._nodes:
            self._nodes[table_path] = Table(self._h5f, table_path)
        return self._nodes[table_path].column(col_name, format)
    
    #analysis.add_metadata('/metadata/trajectory', { 'psf': psf_file, 'pdb': pdb_file, 'dcd': dcd_file, 'frames': num_frames, 'firsttimestep': first_timestep, 'dt': dt })
    def add_metadata(self, path, data, format=tables.StringCol(64)):
        #path is to the table
        #data has the columns
        
        print "Loading metadata..."
        for k, v in data.items():
            col_path = '%s/%s' % (path, k)
            col = self.get_or_create_column(col_path, format)
            col.load(str(v))
        print "Done."
    
    #analysis.add_timeseries('/protein/dihedrals/PEPA_139', Timeseries.Dihedral(trj.selectAtoms("atom PEPA 139 N", "atom PEPA 139 CA", "atom PEPA 139 CB", "atom PEPA 139 CG")))
    def add_timeseries(self, path, timeseries, format=tables.Float32Col()):
        if path in self._timeseries:
            raise Exception('Timeseries with path %s already exists in this analysis!' % path)
        else:
            col = self.get_or_create_column(path, format)
            self._timeseries[path] = (timeseries, col)
   
    #analysis.add_to_sequence('/protein/rmsd/backbone', RMSD(ref, trj, selection='backbone')) 
    def add_to_sequence(self, path, processor, format=tables.Float32Col(), array=False):
        if path in self._sequential:
            raise Exception('Sequential processor with path %s already exists in this analysis!' % path)
        else:
            if array:
                node = self.get_or_create_array(path)
            else:
                node = self.get_or_create_column(path, format)
            self._sequential[path] = (processor, node)
    
    def run(self, trj, ref):
        self._trj = trj
        self._ref = ref
        
        if len(self._timeseries) > 0:
            print "Starting timeseries analysis..."
            collection.clear()
            for path, tpl in self._timeseries.items():
                print " Adding timeseries: %s" % path
                collection.addTimeseries(tpl[0])
            
            print " Computing..."
            collection.compute(self._trj.trajectory)
            print " Done computing."
        
            print "Loading data..."
            for i, path in enumerate(self._timeseries.keys()):
                print " loading table %s with %d values..." % (path, len(collection[i][0]))
                self._timeseries[path][1].load(list(collection[i][0]))
            print "Done timeseries analysis."
        
        if len(self._sequential) > 0:
            print "Running sequential analyses..."
            for path, tpl in self._sequential.items():
                print " Preparing %s" % path
                tpl[0].prepare(ref=self._ref, trj=self._trj)
            frames = self._trj.trajectory
            print " Processing %d frames..." % frames.numframes
            for i, f in enumerate(frames):
                if i % len(frames)/10 == 0:
                    print ".",
                for path, tpl in self._sequential.items():
                    tpl[0].process(f)
            print " done."
            print " Loading result data..."
            for path, tpl in self._sequential.items():
                tpl[1].load(tpl[0].results())
            print "Done sequential analysis."
        
    def save(self):
        print "Setting up and saving all tables and arrays..."
        for path, n in self._nodes.items():
            print " Node: %s" % path
            n.write()
        
    def close(self):
        print "Closing H5 file..."
        self._h5f.flush()
        self._h5f.close()
    
    def open_or_create(self):
        """ 
        Create the pytables data file at the given filename.
        """
        if not os.path.exists(self._filename):
            if self._readonly:
                raise Exception('Read-only open requested on file (%s) that doesn\'t exist!' % self._filename)
            mode = "w"
        else:
            # file exists
            if self._readonly:
                mode = "r"
            else:
                mode = "r+"
        return tables.openFile(self._filename, mode=mode, title=self._title)
