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
