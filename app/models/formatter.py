from app import errors
from ByHelpers import applogger
import json

logger = applogger.get_logger()

class Formatter(object):
    """
        Model to format data in messaged dict
    """

    def __init__(self, params):
        for k in params:
            self.__dict__[k] = params[k]   

    def set_key_type(self, key, data_type):
        self.__dict__[key] = data_type
    
    def process(self, data):
        """ Get raw data to format
        """
        ls_keys = list(data.keys())
        if isinstance(data, dict):
            for k in ls_keys:
                output_type = self.__dict__.get(k, None)
                if output_type:
                    try:
                        if output_type == 'str':
                            data[k] = str(data[k])
                        elif output_type == 'int':
                            data[k] = str(int(str(data[k])))
                        elif output_type == 'json':
                            if isinstance(data[k], list) or isinstance(data[k], dict):
                                continue
                            else:
                                if '[' in str(data[k]) or ']' in str(data[k]) or '{' in str(data[k]) or '}' in str(data[k]):
                                    data[k] = json.loads(str(data[k]))
                                else:
                                    data[k] = str(data[k]).split(',')
                        elif output_type == 'bool':
                            if isinstance(data[k], bool):
                                continue
                            else:
                                data[k] = bool(str(data[k]))
                    except Exception as e:
                        retailer = data['source'] if 'source' in data else 'unknown'
                        logger.error('Error trying to convert {}, {} to {} retailer={}'.format(
                            data[k], k, output_type, retailer))
                        del data[k]
                else:
                    del data[k]
        else:
            logger.error('Data must be a dict')
        return data
