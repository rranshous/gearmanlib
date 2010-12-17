import pickle
import gearman
from decorator import decorator

def get_key(function):
    """
    returns back the gearman key for a function
    """
    # for now we are going to go working dir + function name
    import os
    working_dir = os.path.basename(os.path.abspath('.'))
    function_name = function.__name__
    module_name = function.__module__
    return '%s_%s' % (working_dir,function_name)

class PickleDataEncoder(gearman.DataEncoder):
    @classmethod
    def encode(cls, encodable_object):
        return pickle.dumps(encodable_object)

    @classmethod
    def decode(cls, decodable_string):
        return pickle.loads(decodable_string)

class PickleGearmanClient(gearman.GearmanClient):
    data_encoder = PickleDataEncoder

class PickleGearmanWorker(gearman.GearmanWorker):
    data_encoder = PickleDataEncoder


def call_gearman(key,*args,**kwargs):
    """
    try and find a worker for the given key.
    """
    # TODO: what happens if the worker doesn't exist?
    client = PickleGearmanClient(kwargs.get('hosts',[]) + ['127.0.0.1'])
    if 'hosts' in kwargs:
        del kwargs['hosts']
    r = client.submit_job(key,(args,kwargs))
    print 'result: %s' % r
    return r.result

def gearmanize(function):
    """
    wrap's a function in a call_gearman callable
    """
    def gearmanized(*args,**kwargs):
        return call_gearman(get_key(function),*args,**kwargs)
    return gearmanized


#@decorator
#def farmable(f,*args,**kwargs):

class farmable(object):
    """
    We are trying to enable farming random functions off to gearman 
    if there are any workers available
    """
    def __init__(self,f):
        self.f = f

    def __call__(self,*args,**kwargs):
        f = self.f
        print 'farmable'
        # check and see if we have an async arg
        if kwargs.get('farm',False):
            print 'farming'
            del kwargs['farm']
            # if we are async'n than lets try to farm this bitch out
            callable = gearmanize(f)
        else:
            callable = f

        return callable(*args,**kwargs)
            
