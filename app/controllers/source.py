# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.source import Source
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('source',__name__,url_prefix='/source')


@mod.route('/', methods=["GET"])
@cross_origin(origin="*")
def get_all():
	""" Fetch all Sources in DB
	"""
	logger.info("Fetch all sources...")
	params = request.args.to_dict()
	logger.debug(params)
	# Validate required params
	_needed_params = {'cols'}
	if not _needed_params.issubset(params):
		params['cols'] = ''
	rets = Source.get_all(params['cols'])
	if not rets:
		raise errors.ApiError(70003, "Could not fetch Sources data!")
	return jsonify(rets)

