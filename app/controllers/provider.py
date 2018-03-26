# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.provider import Provider
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('provider',__name__,url_prefix="/provider")


@mod.route('/')
@cross_origin(origin="*")
def get_all():
    """ Testing connection method
            /provider?retailer=ims&p=1&ipp=200
    """
    retailer = request.args.get('retailer') or 'byprice'
    p = None
    ipp = None
    try:
        p = int(request.args.get('p')) if 'p' in request.args else 1
        ipp = int(request.args.get('ipp')) if 'ipp' in request.args else 100000
    except:
        raise errors.ApiError('invalid_data_type', "Query params with wrong data types!")
    provs = Provider.get_all(retailer=retailer, p=p, ipp=ipp)
    if not provs:
        raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Providers")
    return jsonify(provs)

