# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.source import Source
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('source',__name__,url_prefix='/source')


@mod.route('/')
@cross_origin(origin="*")
def get_all():
	"""
		Testing connection method
	"""
	rets = Source.get_all()
	if not rets:
		raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Sources")
	return jsonify(rets)

