from config import *
from flask import g
from ByHelpers import applogger
import sys

logger = applogger.get_logger()

def populate_retailer():
    logger.info("Adding new retailer...")
    try:
        db_ = g._db
    except Exception as e:
        logger.error("Error while getting db: {}".format(e))
        sys.exit()

    name, name_key, type_ = None, None, None
    while not name:
        name = input('Enter the retailer name: ')
        if not name:
            print("Retailer name is mandatory")
        else:
            name = "'" + name + "'"

    while not name_key:
        name_key = input('Enter the retailer key: ')
        if not name_key:
            print("Retailer key is mandatory")
        else:
            name_key = "'" + name_key + "'"

    while not type_:
        type_ = input('Enter the type: ')
        if not type_:
            print("Price type is mandatory Examples. retailer, data_provider")
        else:
            type_ = "'" + type_.lower() + "'"

    retailer = '1' if 'retailer' in type_ else '0'

    logo = input('Enter the logo (Default: {}.png): '.format(name_key.replace("'", "")))
    if logo:
        logo = "'" + logo + "'"
    else:
        logo = "'" + name_key.replace("'", "") + ".png'"

    hierarchy = input('Enter the hierarchy (Default: 10): ')
    if not hierarchy or not hierarchy.isdigit():
        hierarchy = '10'


    query  = """
    INSERT INTO public.source(
	key, name, logo, type, retailer, hierarchy)
	VALUES ({key}, {name}, {logo}, {type_}, {retailer}, {hierarchy});
    """.format(key=name_key, name=name, logo=logo, type_=type_, retailer=retailer, hierarchy=hierarchy)

    logger.info(query)
    try:
        db_.query(query)
    except Exception as e:
        logger.error("Error while inserting retailer: {}".format(e))