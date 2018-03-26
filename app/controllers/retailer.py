# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.retailer import Retailer
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('retailer',__name__,url_prefix='/retailer')


@mod.route('/')
@cross_origin(origin="*")
def get_all():
	"""
		Testing connection method
	"""
	rets = Retailer.get_all()
	if not rets:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Retailers")
	return jsonify(rets)

