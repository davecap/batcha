import numpy
from numpy.linalg import norm
import MDAnalysis.KDTree.NeighborSearch as NS

import logging
logger = logging.getLogger('multicylindersearch')

class MultiCylinderSearch(object):
    """ Count nearby residues of a given type from a given residue selection
    """
    
    def __init__(self, paths, search, radius=10.0, extension=0.0, level='R', update_selections=True):
        """Calculate hydrogen bonds between two selections.

        :Arguments:
          *paths*
            Array of tuples containing [selection for point A, selection for point B] for each cylinder.
          *search*
            Selection for searchable residues/atoms within the cylinder
          *radius*
            Radius of the cylinder
          *extension*
            Extension of the cylinder to search on either side of the two points
          *level*
            R or A (Residue or Atom) for searching. Residues' center of mass are used to calculate distances.
          *update_selections*
            Update selections for points A and B at each frame
            
        The timeseries accessible as the attribute :attr:`CylinderSearch.timeseries`.
        """

        self.paths = paths
        self.selection_search = search
        self.radius = radius
        self.extension = extension
        self.level = level
        self.update_selections = update_selections
    
    def run(self, trj):
        """ Analyze trajectory and produce timeseries. """
        self.timeseries = []
        self.prepare(trj=trj)
        for ts in self.u.trajectory:
            logger.debug("Analyzing frame %d" % ts.frame)
            self.process(ts.frame)
        return self.timeseries

    def prepare(self, ref=None, trj=None):
        """ Prepare the trajectory (trj is a Universe object). No reference object is needed. """
        self.u = trj
        self.u.trajectory.rewind()
        self.paths = [ (self.u.selectAtoms(a), self.u.selectAtoms(b)) for a,b in self.paths ]
        self.search_atomgroup = self.u.selectAtoms(self.selection_search)
        self.timeseries = []  # final result

    def process(self, frame):
        """ Process a single trajectory frame """
        # atomgroup coordinates should update every frame
        results = []
        seen_residues = []
        
        # for each defined path
        for path_index, (sel_a, sel_b) in enumerate(self.paths):
            a = sel_a.centerOfMass()
            b = sel_b.centerOfMass()
            midpoint = (a+b)/2.0
            height = norm(b-a)
            search_radius = height/2.0 + self.extension
        
            # find all selection within r of A and B
            ns = NS.AtomNeighborSearch(self.search_atomgroup)
            near = set(ns.search(midpoint, search_radius, level=self.level))
            for r in near:
                if self.level == 'R':
                    point = r.centerOfMass() # center of mass of the found residue
                    name = '%s:%s' % (r.name, r.id)
                else:
                    point = r.pos
                    name = '%s:%s:%s' % (r.resname, r.resid, r.name)
                
                # skip residues we have seen already
                if name in seen_residues:
                    continue
                                
                distance_to_vector = self._point_distance(a, b, height, point)
                # if the distance to vector is within the radius
                if distance_to_vector <= self.radius:
                    distance_to_a = norm(a-point)
                    distance_to_b = norm(b-point)
                    # calculate the cylinder offset
                    cylinder_offset = numpy.sqrt(distance_to_a**2-distance_to_vector**2)
                    if distance_to_b > height:
                        cylinder_offset = -1*cylinder_offset
                    # add the result to the current path result array
                    results.append((name, path_index, cylinder_offset))
        self.timeseries.append(numpy.array(results))

    def results(self):
        """ Returns an array containing the total count of hbonds per frame """
        return self.timeseries

    def _point_distance(self, a, b, height, point):
        return norm(numpy.cross(point-a, point-b))/height
