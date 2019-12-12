# -*- coding: utf-8 -*-
__version__ = 'v0.0.1'
from flask import Flask, request, jsonify, g, session 
from flask_cors import CORS
import json
import config
from config import *
import datetime
from ByHelpers import applogger
import app.utils.errors as errors
import app.utils.db as db
from app.utils.postgresql import Postgresql as psqldb
from app.utils.proxy import ReverseProxied
if APP_MODE == 'CONSUMER':
    from app import consumer
    from app import consumer_bis

app = Flask(__name__)
app.wsgi_app = ReverseProxied(app.wsgi_app)
app.config.from_object('config')
CORS(app)

# Logger
applogger.create_logger()
logger = applogger.get_logger()


@app.cli.command('new_retailer')
def new_retailer_cmd():
    get_db()
    from scripts.add_new_retailer import populate_retailer
    populate_retailer()

@app.cli.command('consumer')
def consumer_cmd():
    with app.app_context():
        # WIth App ctx, fetch DB connector
        get_db()
        get_psqldb()
        consumer.start()

####TEST
@app.cli.command('consumerb')
def consumerb_cmd():
    with app.app_context():
        # WIth App ctx, fetch DB connector
        get_db()
        get_psqldb()
        consumer_bis.start()


@app.cli.command('initdb')
def initdb_cmd():
    ''' Creates db from cli '''
    db.initdb()
    logger.info("Initialized database")

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


@app.cli.command('init_psqldb')
def init_psqldb_cmd():
    ''' Creates db from cli '''
    psqldb()
    logger.info("Initialized database")


def get_psqldb():
    """ Opens a new database connection if there is none yet for the
        current application context.
    """
    if not hasattr(g, '_psql_db'):
        g._psql_db = psqldb()
    return g._psql_db    


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
    psqldb = getattr(g, '_psql_db', None)
    if psqldb is not None:
        psqldb.connection.commit()
        psqldb.connection.close()


@app.route('/')
def main():
    return jsonify({
        'service' : 'ByPrice Catalogue',
        'author' : 'Byprice {}'.format(str(ENV).lower()),
        'date' : datetime.datetime.utcnow(),
        'version': __version__
    })


# Inject modules
@app.context_processor
def inject_modules():
    # Get main module
    split_path = request.path.split('/')[1:]
    module = 'main' if split_path == [''] else split_path[0]  
    return dict(
        current_module=module,
        modules=['item','product']
    )
   

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
from app.controllers import item, category, source, product, brand, provider, search

app.register_blueprint(item.mod, url_prefix='/item')
app.register_blueprint(product.mod, url_prefix='/product')
app.register_blueprint(search.mod, url_prefix='/search')
app.register_blueprint(category.mod, url_prefix='/category')
app.register_blueprint(source.mod, url_prefix='/source')
app.register_blueprint(brand.mod, url_prefix='/brand')
app.register_blueprint(provider.mod, url_prefix='/provider')


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8002,debug=True)