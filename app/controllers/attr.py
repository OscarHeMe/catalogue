# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.attr import Attr
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('attr',__name__,url_prefix="/attr")


@mod.route('/')
@cross_origin(origin="*")
def get_all():
    """ List of attr classes
    """
    source = request.args.get('source') or 'byprice'
    clsses = Attr.get_clss_list(source=source) 
    if not clsses:
        raise errors.ApiError("catalogue_attr_error", "Could not fetch ingo from attributes")
    return jsonify(ings)

