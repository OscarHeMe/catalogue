# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.item import Item
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('item',__name__,url_prefix="/item")


@mod.route('/test')
def get_one():
	"""
		Testing connection method
	"""
	logger.debug("Testing connection with one product")
	prod = Item.get_one()
	if not prod:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Items")
	return jsonify(prod)

@mod.route('/add', methods=['POST'])
def add_item():
	""" Endpoint to add a new `Item`
	"""
	logger.info("Adding new Item...")
	params = request.get_json()
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'gtin','name','description'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to save Item
	_item = Item(params)
	_item.save()
	return jsonify({
		"status": "OK",
		"message": _item.message,
		"item_uuid": _item.item_uuid
		})


@mod.route('/modify', methods=['POST'])
def modify_item():
	""" Endpoint to modify a new `Item`
	"""
	logger.info("Modify Item...")
	params = request.get_json()
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'item_uuid'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to save Item
	_item = Item(params)
	_item.save()
	return jsonify({
		"status": "OK",
		"message": _item.message,
		"item_uuid": _item.item_uuid
		})


@mod.route('/delete', methods=['GET'])
def delete_item():
	""" Endpoint to delete an `Item` by item_uuid
	"""
	logger.info("Delete Item...")
	params = request.args
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'uuid'}
	if not _needed_params.issubset(params):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to delete Item
	_resp = Item.delete(params['uuid'])
	return jsonify({
		"status": "OK",
		"message": _resp['message']
		})



@mod.route('/details', methods=['GET'])
def details_item(items_str):
	""" Endpoint to get details of given items
		
		@Params: 
			- values: <str> list of item_uuids comma separated
			- by: <str> field which the values are queried against
				(WHERE <by> = <value>)
			- cols: <str> can be item_uuid, gtin, name, description 

		@Response:
			- resp: items list
	"""
	logger.info("Items details")
	params = request.args
	# Validation
	if not params:
		raise errors.ApiError(70001, "Missing required key params")

	# Verify needed key-values
	_needed_params = {'values','by'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")

	if 'cols' not in params:
		cols = ['item_uuid','gtin','name','description']
	
	# Call to delete Item
	values = params['values'].split(",")
	by = params['by']
	_resp = Item.get(values, by=by, _cols=cols)
	return jsonify({
		"status": "OK",
		"message": _resp['message'],
		"items" : _resp
		})
