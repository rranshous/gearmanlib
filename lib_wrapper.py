#!/usr/bin/python

# our goal is to be able to specify a python module / package
# to import and than all it's functions become available as
# gearman worker functions

# to start small we'll take two cmd line args one will be
# the name of the module the other will be what to import and make
# available
import os
print os.path.abspath('.')
import sys
sys.path.append(os.path.abspath('.'))
module = sys.argv[1]
func = sys.argv[2]
m = __import__(module)
f = getattr(m,func)
import gearman
from gearman_helpers import PickleGearmanWorker
gm_worker = PickleGearmanWorker(['localhost'])
#gm_worker.set_client_id('your_worker_client_id_name')
def decode(gearman_worker,gearman_job):
    try:
        data = gearman_job.data
        if type(data) is tuple:
            print 'tuple'
            return f(*data)
        return f(data)
    except Exception, ex:
        print 'exception: %s' % ex
        raise
gm_worker.register_task('%s_%s' % (module,func), decode)

# Enter our work loop and call gm_worker.after_poll() after each time we timeout/see socket activity
print 'working on %s_%s' % (module,func)
gm_worker.work()
