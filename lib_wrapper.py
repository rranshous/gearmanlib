#!/usr/bin/python

# our goal is to be able to specify a python module / package
# to import and than all it's functions become available as
# gearman worker functions

# to start small we'll take two cmd line args one will be
# the name of the module the other will be what to import and make
# available

if __name__ == '__main__':
    from gearman_helpers import get_key, HOSTS
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
    gm_worker = PickleGearmanWorker(HOSTS)
    #gm_worker.set_client_id('your_worker_client_id_name')
    def decode(gearman_worker,gearman_job):
        try:
            data = gearman_job.data
            # if your a tuple than we want to expand you
            if type(data) is tuple:
                # if you are a tuple of a tuple and a dict than
                # we will also kw expand you
                if len(data) == 2 and isinstance(data[0],tuple) and \
                                      isinstance(data[1],dict):
                    return f(*data[0],**data[1])
                else:
                    return f(*data)
            return f(data)
        except Exception, ex:
            print 'exception: %s' % ex
            raise
    key = get_key(f)
    gm_worker.register_task(key, decode)

    # Enter our work loop and call gm_worker.after_poll() after each time we timeout/see socket activity
    print 'working on %s' % key
    gm_worker.work()
