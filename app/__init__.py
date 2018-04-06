# -*- coding: utf-8 -*-
__version__ = 'v0.0.1'
from flask import Flask, request, jsonify, g, session 
from flask_cors import CORS
import json
import config
from config import *
import datetime
import app.utils.applogger as applogger
import app.utils.errors as errors
import app.utils.db as db

app = Flask(__name__)
app.config.from_object('config')
CORS(app)

# Logger
applogger.create_logger()
logger = applogger.get_logger()
   

@app.cli.command('initdb')
def initdb_cmd():
    db.initdb()

@app.cli.command('dropdb')
def dropdb_cmd():
    db.dropdb()

def initdb():
    db.initdb()

def dropdb():
    db.dropdb()

def get_db():
    """ Opens a new database connection if there is none yet for the
        current application context.
    """
    if not hasattr(g, '_db'):
        g._db = db.connectdb()
    return g._db


# Connect to PostgreSQL Items DB
# Before requests
@app.before_request
def before_request():
    logger.debug("Before Request")
    g._db = get_db()

# After requests
@app.teardown_appcontext
def close_db(error):
    ''' 
        Close connection at the end of every request
    '''
    logger.debug("Teardown Method")
    db = getattr(g, '_db', None)
    if db is not None:
        db.close()
        
        
@app.cli.command('initdb')
def initdb_cmd():
    ''' Creates db from cli '''
    db.initdb()
    logger.info("Initialized database")


@app.route('/')
def main():
    return jsonify({
        'service' : 'ByPrice Catalogue',
        'author' : 'Byprice Dev',
        'date' : datetime.datetime.utcnow(),
        'version': __version__
    })

#Error Handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "not found"
    }), 400

# HTTP Error handling
@app.errorhandler(400)
def bad_request(error):
    return jsonify({
        "error": "bad request"
    }), 400

# API errors
@app.errorhandler(errors.ApiError)
def handle_api_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

# Importing blueprint modules
from app.controllers import item, category, source, product

app.register_blueprint(item.mod, url_prefix='/item')
app.register_blueprint(product.mod, url_prefix='/product')
app.register_blueprint(category.mod, url_prefix='/category')
app.register_blueprint(source.mod, url_prefix='/source')



if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8002,debug=True)