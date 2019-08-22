# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.search import Search
from app import errors, logger
import io
import pandas as pd
from flask_cors import CORS, cross_origin

mod = Blueprint('search',__name__,url_prefix='/search')


@mod.route('/', methods=["GET"])
def get_root():
	"""
	Root route
	"""
	message = {
		'status': 'ok',
		'message': 'Products to search'
	}
	return jsonify(message)


@mod.route('/products/by/source', methods=["GET"])
def get_prods():
	"""
	Fetch all products to search from source(s)
	"""
	logger.info("Query Product to search...")
	params = request.args.to_dict()
	_needed_params = {'keys'}
	if not _needed_params.issubset(params):
		raise errors.ApiError(70001, "Missing required key params")
	sources = params['keys']
	# Complement optional params, and set default if needed
	_opt_params = {'p':1, 'ipp': 50}
	for _o, _dft in _opt_params.items():
		if _o not in params:
			params[_o] = _dft
	_prods = Search.get_by_source(sources, **dict(params))
	return jsonify({
        'status': 'OK',
        'products': _prods
        })


@mod.route('/products/save', methods=["POST"])
def add_prods():
	"""" Endpoint to add a new product
    """
	logger.debug("Adding new product...")
	params = request.form
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'name', 'source'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")
    # Call to save product
	srch_prod = Search(params)
	_saved = srch_prod.add()
	message = 'Product {}saved'.format('' if _saved else 'not ')
	return jsonify({
        "status": "OK",
        "message": message,
    })


### In development
# @mod.route('/products/csv', methods=["POST"])
# def csv_prods():
# 	"""" Endpoint to add a new product from csv file
#     """
# 	logger.debug("Reading CSV...")
# 	_file = request.files.get('data')
# 	if _file:
# 		data = _file.stream.read()
# 		print(type(data))
# 	try:
# 		# f = request.files['data_file']
# 		if True:
# 			# output = io.StringIO(f.stream.read().decode("UTF8"), newline=None)
# 			# df = pd.read_csv(output)
# 			_saved = None
# 			message = 'Products {}saved'.format('' if _saved else 'not ')
# 		else:
# 			message = "No file"
# 	except Exception as e:
# 		message = str(e)
		
# 	return jsonify({
#         "status": "OK",
#         "message": message,
#     })