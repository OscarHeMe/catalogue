# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.ingredient import Ingredient
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('ingredient',__name__,url_prefix="/ingredient")


@mod.route('/')
@cross_origin(origin="*")
def get_all():
    """ List of ingredients
    """
    retailer = request.args.get('retailer') or 'byprice'
    ings = Ingredient.get_all(retailer=retailer)
    if not ings:
        raise errors.ApiError("invalid_request", "Could not fetch data from Postgres Providers")
    return jsonify(ings)

