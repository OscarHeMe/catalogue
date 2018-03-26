# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.item import Item
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('item',__name__,url_prefix="/item")


@mod.route('/')
def get_one():
	"""
		Testing connection method
	"""
	logger.debug("Testing connection with one product")
	prod = Item.get_one()
	if not prod:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Items")
	return jsonify(prod)

@mod.route('/<item_id>', methods=['GET'])
def get_by_id(item_id):
	"""
		Controller to get item by item_uuid
	"""
	logger.debug("Fetching item info by id...")
	retailer = request.args.get('retailer',None)
	prod = Item.get_by_id(item_id, request.args.get('retailer'))
	if not prod:
		raise errors.ApiError("item_issues", "Unable to fetch item's info")
	return jsonify(prod)

@mod.route('/catalogue', methods=['GET'])
def  catalogue():
	"""
		Get all items from an especific retailer *ret* and it can paginate with the *p* param 
		and stabilsh number of items through *ipp* .

		Requests params
		* p = Page Number (integer)
		* ipp = Items Per Page (integer)

		Response structure
		* items = List of items with JSON structure (list)
		* missing = Number of items not yet fetched (integer)
	"""
	logger.info("Fetching Catalogue items per retailer")
	try:
		p = int(request.args.get('p')) if 'p' in request.args else 1
		ipp = int(request.args.get('ipp')) if 'ipp' in request.args else 200
	except:
		raise errors.ApiError('invalid_data_type', "Query params with wrong data types!")
	# Call function to obtain items from certain retailer
	catalogue = Item.get_catalogue(p, ipp)
	if catalogue['status'] == 'ERROR':
		raise errors.ApiError("invalid_request", catalogue['msg'])
	return jsonify(catalogue['data'])
	

@mod.route('/categories')
def get_categories():
	"""
		Get all categories
	"""
	cats = Item.get_categories()
	return jsonify({"categories":cats})


@mod.route('/info/<item_uuid>', methods=['GET'])
def get_info(item_uuid):
	"""
		Get all categories
	"""
	info = Item.fetch_info(item_uuid)
	return jsonify(info)

@mod.route('/gs1', methods=['GET'])
def get_list_gs1():
	"""
		Get all items from gs1s
	"""
	p = request.args.get('p') if 'p' in request.args else 0
	info = Item.list_gs1(p)
	return jsonify(info)


@mod.route('/get_uuid', methods=['GET'])
def get_uuid():
	''' Get item_uuid given the external id and retailer
		gate.byprice.com/public/item/item/get_uuid?retailer=farmacias_similares&item_id=00000000000000001507
	'''
	item_id = request.args.get('item_id')
	retailer = request.args.get('retailer')
	item = Item.get_details(item_id=item_id, retailer=retailer)
	return jsonify(item)

@mod.route('/get_uuids', methods=['POST'])
def get_uuids():
	''' 
		Get item_uuids given the GTINs list completed to 14 digits
		Params:
		{
			'gtins': [
					'56373563',
					'024758387'
				]
		}
	'''
	q = request.get_json()
	if not q:
		raise errors.ApiError('missing_params', "Query params missing!")
	if 'gtins' not in q:
		raise errors.ApiError('missing_params', "Missing 'gtins' Key in params query!")
	items = Item.get_items_by_gtin(q['gtins'])
	return jsonify(items)


@mod.route('/retailer')
def get_by_retailer():
	"""
		Get all categories
	"""
	retailer = request.args.get('retailer')
	categories = request.args.get('categories')
	id_categories = categories.split(",") if categories else None

	items = Item.get_by_retailer(retailer, fields=["item_uuid","name","gtin"], categories=id_categories)
	return jsonify(items)


@mod.route('/filtered', methods=['POST'])
@cross_origin(origin="*")
def get_by_filters():
	"""
		Get items given some filters
	"""
	filters = request.get_json()
	print(filters)
	'''
	filters = [
		{ "category" : "9406" },
		{ "category" : "9352" },
		{ "category" : "8865" },
		{ "retailer" : "walmart" },
		{ "retailer" : "superama" },
		{ "retailer" : "ims" },
		{ "item" : "67e8bc34-2e0d-460b-8ed0-72710b19f1b6" },
		{ "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" }
	]
	'''
	items = Item.get_by_filters(filters)
	return jsonify(items)

@mod.route('/catalogue/filter', methods=['GET'])
def  catalogue_filter():
	"""
		Get all items from an especific retailer *ret* and it can paginate with the *p* param 
		and stabilsh number of items through *ipp* .

		# Requests params
		* ui = Item UUID  (str)

		# Response structure
		* items = Item params with JSON structure (dict)
	"""
	logger.info("Fetching Catalogue from Item..")
	try:
		_ui = str(request.args.get('ui'))
	except:
		raise errors.ApiError('invalid_data_type', "Missing UUID query param!")
	# Call function to obtain item by item_uuid
	catalogue = Item.filtered_cat(_ui)
	if catalogue['status'] == 'ERROR':
		raise errors.ApiError("invalid_request", catalogue['msg'])
	return jsonify(catalogue['data'])