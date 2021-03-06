# -*- coding: utf-8 -*-
from ByHelpers import applogger
from pprint import pformat as pf
from .normalize_text import key_format
import ast
import json

# Logger
logger = applogger.get_logger()


def list_to_str(_list):
    """ Convert lists and a-like lists into
        comma separated string

        Params: 
        -----
        _list : str | list
            Param to convert

        Returns:
        -----
        _vals : str
            Comma-separated string
    """
    if isinstance(_list, list):
        return ','.join(_list)
    elif isinstance(_list, str):
        try:
            _vals = ast.literal_eval(_list)
        except:
            _vals = _list if (str(_list) != 'None') and (_list) else ''
        return _vals
    return ''

def product(r_key, data):
    """ Map `product` depending on route key to reformat into 
        what is needed.

        Params: 
        -----
        r_key : str
            Route Key
        data : dict
            Product info

        Returns:
        -----
        fdata : dict
            Reformatted Product info
    """
    if r_key == 'item':
        return item(data)
    elif r_key == 'price':
        return price(data)

def item(data):
    """ Map `item` keys to reformat into 
        what is needed.

        Params: 
        -----
        data : dict
            Product info

        Returns:
        -----
        fdata : dict
            Reformatted Product info
    """
    #logger.debug(pf(data))
    if not isinstance(data, dict):
        return {}
    fdata = {}
    # Remap keys without modification
    re_map = {'url': 'url', 'retailer_key': 'source', 'retailer': 'source',
              'provider': 'provider', 'brand': 'brand', 'name': 'name',
              'description': 'description', 'raw_html': 'raw_html',
              'id': 'product_id', 'gtin': 'gtin', 'is_active': 'is_active'}
    for _old, _new in re_map.items():
        if _old in data:
            fdata[_new] = data[_old]
    # Validate some params
    if 'images' in data:
        fdata['images'] = data['images'] \
            if isinstance(data['images'], list) \
            else [data['images']]
    if 'name' in fdata:
        if not fdata['name']:
            if 'description' in fdata:
                fdata['name'] = fdata['description'] if fdata['description'] else ''
    # Reformat specific cols
    if 'categories' in data:
        fdata['categories'] = list_to_str(data['categories'])
    if 'match' in data:
        if 'gtin' in data['match']:
            fdata['gtin'] = data['match']['gtin']
    fdata['attributes'] = []
    if 'attributes' in data:
        if isinstance(data['attributes'], list):
            _fatrs = []
            _nparams = {'attr_name', 'attr_key', 'clss_name', 'clss_key'}
            for _atts in data['attributes']:
                if _nparams.issubset(_atts):
                    _fatrs.append(_atts)
            fdata['attributes'] = _fatrs
    if 'ingredients' in data:
        if isinstance(data['ingredients'], list):
            _fing = []
            for _ing in data['ingredients']:
                if _ing:
                    _fing.append({
                        "attr_name": _ing,
                        "attr_key": key_format(_ing),
                        "clss_name": "Ingrediente",
                        "clss_key": "ingredient",
                    })
            fdata['attributes'] += _fing
    # Set raw data
    try:
        fdata['raw_product'] = json.dumps(data)
    except Exception as e:
        logger.error(e)
        logger.warning("Could not serialize all product data to JSON!")
    return fdata


def price(data):
    """ Map `price` keys to reformat into 
        what is needed.

        Params: 
        -----
        data : dict
            Product info

        Returns:
        -----
        fdata : dict
            Reformatted Price info
    """
    #logger.debug(pf(data))
    if not isinstance(data, dict):
        return {}
    fdata = {}
    # Remap keys that need different column name
    re_map = {'retailer': 'source', 'id': 'product_id'}
    for _key, _val in data.items():
        if _key in re_map:
            fdata[re_map[_key]] = _val
        else:
            fdata[_key] = _val
    # Validate some params
    if 'name' in data:
        fdata['name'] = data['name'] if data['name'] else ''
    else:
        fdata['name'] = ''
    return fdata