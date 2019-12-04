from app import errors, logger
import json

class Formatter(object):
    """
        Model to format data in messaged dict
    """

    def __init__(self, **kwargs):
        for k in kwargs:
            self.__dict__[k] = kwargs[k]   

    def set_key_type(self, key, data_type):
        self.__dict__[key] = data_type
    
    def process(self, data):
        """ Get raw data to format
        """
        if isinstance(data, dict):
            for k in data.keys():
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
                                data[k] = json.loads(str(data[k]))
                        elif output_type == 'bool':
                            if isinstance(data[k], bool):
                                continue
                            else:
                                data[k] = bool(str(data[k]))
                    except Exception as e:
                        logger.error('Error trying to convert {} to {}'.format(data[k], output_type))
                        del data[k]
                else:
                    del data[k]
        else:
            logger.error('Data must be a dict')
        return data
