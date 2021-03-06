#-*-coding: utf-8-*-
from pygres import Pygres
import config
from config import * 
import os
import logging

logger = logging.getLogger(APP_NAME)

# Database creation
def initdb():
    ''' Initialize the db '''
    # Define schema
    _schema = 'schema.sql' if not TESTING else 'schema.sql'
    try:
        db_init = Pygres(dict(
            SQL_DB='postgres',
            SQL_USER=SQL_USER,
            SQL_PASSWORD=SQL_PASSWORD,
            SQL_HOST=SQL_HOST,
            SQL_PORT=SQL_PORT
        ),autocommit=True)
        try:
            db_init.query('create database {}'.format(config.SQL_DB))
            logger.info("Created DB!")
        except:
            raise Exception("Db was already initialized!")
        db_init.close()
        del db_init
        # insert the tables
        db_init = Pygres(dict(
            SQL_HOST = SQL_HOST,
            SQL_DB = config.SQL_DB,
            SQL_USER = SQL_USER,
            SQL_PASSWORD = SQL_PASSWORD,
            SQL_PORT=SQL_PORT,
        ))
        with open( BASE_DIR + '/'+_schema,'r') as f:
            try:
                db_init.query(f.read())
                logger.info("Schema generated in DB!")
            except Exception as e:
                raise Exception(str(e))
        db_init.close()
    except Exception as e:
        logger.info(e)


def connectdb():
    """Connects to the specific database."""
    logger.debug("Trying to connect to")
    logger.debug( str([SQL_HOST, config.SQL_DB, SQL_USER, SQL_PORT]) )
    return Pygres(dict(
        SQL_HOST = SQL_HOST,
        SQL_DB = config.SQL_DB,
        SQL_USER = SQL_USER,
        SQL_PASSWORD = SQL_PASSWORD, 
        SQL_PORT=SQL_PORT,
    ))


def dropdb():
    ''' Drops the testing database '''
    if not config.TESTING:
        return None
    db_drop = Pygres(dict(
        SQL_DB='postgres',
        SQL_USER=SQL_USER,
        SQL_PASSWORD=SQL_PASSWORD,
        SQL_HOST=SQL_HOST,
        SQL_PORT=SQL_PORT
    ),autocommit=True)
    db_drop.query('drop database {}'.format(config.SQL_DB))
    logger.info("Database dropped")
    db_drop.close()


def getdb():
    """ Opens a new database connection if there is none yet for the
        current application context.
    """
    logger.info("Getting session... ")
    session = connectdb()
    return session


''' If it's main, create the database
'''
if __name__ == '__main__':
    initdb()