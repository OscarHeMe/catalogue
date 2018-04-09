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
	jsonify({"status": "in_construction"})


@mod.route('/modify', methods=['POST'])
def modify_item():
	""" Endpoint to modify a new `Item`
	"""
	jsonify({"status": "in_construction"})


@mod.route('/delete', methods=['GET'])
def delete_item():
	""" Endpoint to delete a new `Item`
	"""
	jsonify({"status": "in_construction"})
