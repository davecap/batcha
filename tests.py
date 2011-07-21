from MDAnalysis import *
from MDAnalysis.tests.datafiles import PSF,DCD

from batcha import Analysis

def test_run():
    print "Loading reference system: %s, %s" % (PSF, DCD)
    ref = Universe(PSF, DCD, permissive=True)
    print "Loading trajectory: %s" % (DCD)
    trj = Universe(PSF, DCD, permissive=True)
    
    # add the metadata for this analysis to the database
    analysis = Analysis('test.h5', readonly=False)
    
    # Test metadata
    analysis.add_metadata('/metadata/test', { 'a': '1', 'b': '2' })
    analysis.add_metadata('/metadata/test1', { 'a': '1', 'b': '2' })
    analysis.add_metadata('/metadata/test2', { 'a': '1', 'b': '2' })
    
    # Test timeseries
    analysis.add_timeseries('/timeseries/com/COM_ALL', Timeseries.CenterOfMass(ref.atoms))
    analysis.add_timeseries('/timeseries/com/COM_ALL1', Timeseries.CenterOfMass(ref.atoms))
    analysis.add_timeseries('/timeseries/com/COM_ALL2', Timeseries.CenterOfMass(ref.atoms))
    
    analysis.run(trj=trj, ref=ref)
    analysis.save()
    analysis.close()

test_run()
