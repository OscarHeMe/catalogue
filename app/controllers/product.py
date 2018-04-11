# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.product import Product
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('product',__name__,url_prefix="/product")


@mod.route('/test')
def get_one():
	""" Testing connection method
	"""
	logger.debug("Testing connection with one product")
	prod = Product.get_one()
	if not prod:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Items")
	return jsonify(prod)


@mod.route("/add", methods=['POST'])
def add_prod():
	""" Endpoint to add a new `Product` with respective,
		product images, attributes and categories.
	"""
	logger.info("Adding new Product...")
	params = request.get_json()
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'source','product_id', 'name', 'description'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to save Product
	_prod = Product(params)
	_prod.save()
	return jsonify({
		"status": "OK",
		"message": _prod.message,
		"product_uuid": _prod.product_uuid
		})


@mod.route("/modify", methods=['POST'])
def modify_prod():
	""" Endpoint to modify a `Product` with respective,
		product images, attributes and categories.
	"""
	logger.info("Modify existing Product...")
	params = request.get_json()
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'product_uuid'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to save Product
	_prod = Product(params)
	_prod.save()
	return jsonify({
		"status": "OK",
		"message": _prod.message,
		"product_uuid": _prod.product_uuid
		})


@mod.route("/image", methods=['POST'])
def update_img_prod():
	""" Endpoint to update a `Product Image`.
	"""
	logger.info("Update Product image...")
	params = request.get_json()
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'product_uuid', 'image'}
	if not _needed_params.issubset(params.keys()):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to update prod image
	return jsonify({
		'status': 'OK',
		'message': Product.update_image(params)['message']
	})


@mod.route("/by/iuuid", methods=['GET'])
def get_byitem():
	""" Endpoint to fetch `Product`s by item_uuid's.
	"""
	return jsonify({'status': 'in_construction'})


@mod.route("/by/puuid", methods=['GET'])
def get_byprod():
	""" Endpoint to fetch `Product`s by product_uuid's.
	"""
	return jsonify({'status': 'in_construction'})


@mod.route("/by/source", methods=['GET'])
def get_bysource():
	""" Endpoint to fetch `Product`s by source's.
	"""
	return jsonify({'status': 'in_construction'})


@mod.route("/by/attr", methods=['GET'])
def get_byattr():
	""" Endpoint to fetch `Product`s by attr's.
	"""
	return jsonify({'status': 'in_construction'})


@mod.route("/delete", methods=['GET'])
def delete_prod():
	""" Endpoint to delete a `Product`s by product_uuid.
	"""
	logger.info("Delete Product...")
	params = request.args
	logger.debug(params)
	if not params:
		raise errors.ApiError(70001, "Missing required key params")
	# Verify needed key-values
	_needed_params = {'uuid'}
	if not _needed_params.issubset(params):
		raise errors.ApiError(70001, "Missing required key params")
	# Call to delete Item
	_resp = Product.delete(params['uuid'])
	return jsonify({
		"status": "OK",
		"message": _resp['message']
		})



@mod.route("/delete/attr", methods=['GET'])
def delete_prod_attr():
	""" Endpoint to delete `Product`s attribute by product_uuid 
		and  attribute key.
	"""
	return jsonify({'status': 'in_construction'})