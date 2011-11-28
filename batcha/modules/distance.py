import numpy
import logging
logger = logging.getLogger('distance')

class DistanceAnalysis(object):
    """ Calculate distance between two selections
    """
    
    def __init__(self, selection1, selection2):
        """Calculate distance between two selections.

        :Arguments:
          *selection1*
            Selection string for first selection
          *selection2*
            Selection string for second selection
            
        The timeseries accessible as the attribute :attr:`DistanceAnalysis.timeseries`.
        """

        self.selection1 = selection1
        self.selection2 = selection2
        
        if not (self.selection1 and self.selection2):
            raise Exception('DistanceAnalysis: invalid selections')

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
        self._update_selections()
        self.timeseries = []  # final result

    def process(self, frame):
        """ Process a single trajectory frame """
        r = self._s1.centerOfMass() - self._s2.centerOfMass()
        d = numpy.sqrt(numpy.sum(r*r))
        self.timeseries.append(d)
        return d 

    def results(self):
        """ Returns an array containing the total count of nearby atoms """
        return self.timeseries

    def _update_selections(self):
        self._s1 = self.u.selectAtoms(self.selection1)
        self._s2 = self.u.selectAtoms(self.selection2)

