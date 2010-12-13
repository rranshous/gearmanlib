import pickle
import gearman

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


def call_gearman(func_name,*args):
    client = PickleGearmanClient(['127.0.0.1'])
    return client.submit_job(func_name,args).result

