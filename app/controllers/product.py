# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.product import Product
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('product',__name__,url_prefix="/product")


@mod.route('/test')
def get_one():
	"""
		Testing connection method
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
	jsonify({'status': 'in_construction'})


@mod.route("/modify", methods=['POST'])
def modify_prod():
	""" Endpoint to modify a `Product` with respective,
		product images, attributes and categories.
	"""
	jsonify({'status': 'in_construction'})


@mod.route("/by_iuuid", methods=['GET'])
def get_byitem():
	""" Endpoint to fetch `Product`s by item_uuid's.
	"""
	jsonify({'status': 'in_construction'})


@mod.route("/by_puuid", methods=['GET'])
def get_byprod():
	""" Endpoint to fetch `Product`s by product_uuid's.
	"""
	jsonify({'status': 'in_construction'})


@mod.route("/by_attr", methods=['GET'])
def get_byattr():
	""" Endpoint to fetch `Product`s by attr's.
	"""
	jsonify({'status': 'in_construction'})


@mod.route("/delete", methods=['GET'])
def delete_prod():
	""" Endpoint to delete `Product`s by product_uuid.
	"""
	jsonify({'status': 'in_construction'})


@mod.route("/delete/attr", methods=['GET'])
def delete_prod():
	""" Endpoint to delete `Product`s attribute by product_uuid 
		and  attribute key.
	"""
	jsonify({'status': 'in_construction'})
