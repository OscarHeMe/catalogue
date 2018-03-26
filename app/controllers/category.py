# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.category import Category
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('category',__name__, url_prefix="/category")



@mod.route('/')
@cross_origin(origin="*")
def get_all():
	"""
		Get all categories from a given retailer
	"""
	logger.debug("Testing connection with one product")
	retailer = ctype = request.args.get('retailer')
	cats = Category.get_all(retailer=retailer)
	if not cats:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Categories")
	return jsonify(cats)



@mod.route('/ims', methods=['GET'])
@cross_origin(origin="*")
def ims():
	"""
		Get ims categories given the level and type 
	"""
	logger.info("Fetching Catalogue items per retailer")
	try:
		ctype = request.args.get('type') or 'atc'
		levels = request.args.get('levels').split(",") or [3,4]
		nested = request.args.get('nested') == '1' or False
	except:
		raise errors.ApiError('invalid_data_type', "Query params with wrong data types!")
	# Call function to obtain items from certain retailer
	categories = Category.get_ims(ctype=ctype, levels=levels, nested=nested)
	return jsonify(categories)
	

