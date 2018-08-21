# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.source import Source
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('source',__name__,url_prefix='/source')


@mod.route('/', methods=["GET"])
@cross_origin(origin="*")
def get_all():
	""" Fetch all Sources in DB
	"""
	logger.info("Fetch all sources...")
	params = request.args.to_dict()
	logger.debug(params)
	# Validate required params
	_needed_params = {'cols'}
	if not _needed_params.issubset(params):
		params['cols'] = ''
	rets = Source.get_all(params['cols'])
	if not rets:
		raise errors.ApiError(70003, "Could not fetch Sources data!")
	return jsonify(rets)


@mod.route("/products/<source>", methods=['GET'])
def get_intersection(source):
	""" Endpoint to fetch `Product`s by attr's.

		/intersect?<field1>=<values>&<field2>=<values>
		translates to:
		where <field1> in (<vals>) and <field2> in (<vals>)

		@Request:
		- <field>=<values> : n number of fields and values to make the qry
		- cols : columns
		- p : page
		- ipp : items per page

		@Response:
		- products

	"""
	logger.info("Query source catalogue...")
	params = request.args.to_dict()
	params['source'] = source
	logger.debug(params)

	# The keys of the params are the fields
	if not params:
		logger.error(70001, "No params to query with")

	# Pagination default
	if not 'p' in params:
		params['p'] = 1
	if not 'ipp' in params:
		params['ipp'] = 100
        
	# Query items
	_prods = Source.get_products(**params)
        
	return jsonify({
		'status': 'OK',
		'products': _prods
		})