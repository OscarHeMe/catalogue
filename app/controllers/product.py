# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.product import Product
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('product',__name__,url_prefix="/product")


@mod.route('/')
def get_one():
	"""
		Testing connection method
	"""
	logger.debug("Testing connection with one product")
	prod = Product.get_one()
	if not prod:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Items")
	return jsonify(prod)