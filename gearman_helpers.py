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

HOSTS = ['gearman.mypubliccode.com']

def get_client(host=[]):
    client = PickleGearmanClient(host + HOSTS)
    return client

def call_gearman(key,*args,**kwargs):
    """
    try and find a worker for the given key.
    """
    # TODO: what happens if the worker doesn't exist?
    client = get_client(kwargs.get('host'))
    if 'hosts' in kwargs:
        del kwargs['hosts']
    r = client.submit_job(key,(args,kwargs))
    print 'result: %s' % r
    return r.result

def gearmanize(function,background=False):
    """
    wrap's a function in a call_gearman callable
    """
    def gearmanized(*args,**kwargs):
        print 'background: %s' % background
        client = get_client()
        r = client.submit_job(get_key(function),(args,kwargs),
                              background=background)
        return r
    return gearmanized


# TODO: add support for local background using threads
def gearmanize_if_possible(f,timeout=5*60):
    """
    wrap a function so that it will try and be farmed
    out to gearman, if there is no worker or it errors
    for some other reason try to run it locally
    """
    def gearmanized_if_possible(*args,**kwargs):
        # get a client
        key = get_key(f)
        client = get_client()
        r = client.submit_job(key,(args,kwargs),poll_timeout=timeout)
        if r.complete and not r.timed_out:
            to_return = r.result
        else:
            print 'not complete running locally'
            to_return = f(*args,**kwargs)
        return to_return
    return gearmanized_if_possible


#@decorator
#def farmable(f,*args,**kwargs):

class farmable(object):
    """
    We are trying to enable farming random functions off to gearman 
    if there are any workers available
    """
    def __new__(cls,f,*args,**kwargs):
        inst = object.__new__(cls,*args,**kwargs)
        inst.__name__ = f.__name__
        return inst

    def __init__(self,f):
        self.f = f

    def __call__(self,*args,**kwargs):

        # our function to run
        f = self.f

        print 'farmable'

        # pull out our special variables
        farm_args = ('farm','background','timeout')
        params = {}
        for arg in farm_args:
            params[arg] = kwargs.get(arg)
            print 'del: %s' % arg
            if arg in kwargs:
                del kwargs[arg]

        print 'kwargs: %s' % kwargs

        # if we got a farm flag, than do it
        if params.get('farm'):
            print 'farming'
            # if we are async'n than lets try to farm this bitch out
            if params.get('background'):
                print 'background: %s' % params.get('background')
                callable = gearmanize(f,background=params.get('background'))
            else:
                callable = gearmanize_if_possible(f,timeout=params.get('timeout'))
        else:
            callable = f

        return callable(*args,**kwargs)
            
