import pickle
import gearman
from decorator import decorator

def get_key(function):
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
    client = PickleGearmanClient(kwargs.get('hosts',[]) + ['127.0.0.1'])
    del kwargs['hosts']
    r = client.submit_job(key,(args,kwargs))
    print 'result: %s' % r
    return r.result

def gearmanize(function):
    def gearmanized(*args,**kwargs):
        return call_gearman(get_key(function),*args,**kwargs)
    return gearmanized

@decorator
def asycable(f,*args,**kwargs):
    """
    We are trying to enable farming random functions off to gearman 
    if there are any workers available
    """
    # check and see if we have an async arg
    if kwargs.get('async',False):
        pass
    # if we are async'n than lets try to farm this bitch
    # out
    result = call_gearman()
        
