# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.product import Product
from app import errors, logger
from flask_cors import CORS, cross_origin

mod = Blueprint('product',__name__,url_prefix="/product")


@mod.route('/test')
def get_one():
    """ Testing connection method
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
    logger.info("Adding new Product...")
    params = request.get_json()
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'source','product_id', 'name', 'description'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to save Product
    _prod = Product(params)
    _prod.save()
    return jsonify({
        "status": "OK",
        "message": _prod.message,
        "product_uuid": _prod.product_uuid
        })


@mod.route("/modify", methods=['POST'])
def modify_prod():
    """ Endpoint to modify a `Product` with respective,
        product images, attributes and categories.
    """
    logger.info("Modify existing Product...")
    params = request.get_json()
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'product_uuid'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to save Product
    _prod = Product(params)
    _prod.save()
    return jsonify({
        "status": "OK",
        "message": _prod.message,
        "product_uuid": _prod.product_uuid
        })


@mod.route("/image", methods=['POST'])
def update_img_prod():
    """ Endpoint to update a `Product Image`.
    """
    logger.info("Update Product image...")
    params = request.get_json()
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'product_uuid', 'image'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to update prod image
    return jsonify({
        'status': 'OK',
        'message': Product.update_image(params)['message']
    })


@mod.route("/by/iuuid", methods=['GET'])
def get_byitem():
    """ Endpoint to fetch `Product`s by item_uuid's.
    """
    logger.info("Query Product by Item UUID...")
    params = request.args.to_dict()
    logger.debug(params)
    # Validate required params
    _needed_params = {'keys'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Complement optional params, and set default if needed
    _opt_params = {'cols': '', 'p':1, 'ipp': 50}
    for _o, _dft  in _opt_params.items():
        if _o not in params:
            params[_o] = _dft
    _prods = Product.query('item_uuid', **params)
    return jsonify({
        'status': 'OK',
        'products': _prods
        })


@mod.route("/by/puuid", methods=['GET'])
def get_byprod():
    """ Endpoint to fetch `Product`s by product_uuid's.
    """
    logger.info("Query Product by Product UUID...")
    params = request.args.to_dict()
    logger.debug(params)
    # Validate required params
    _needed_params = {'keys'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Complement optional params, and set default if needed
    _opt_params = {'cols': '', 'p':1, 'ipp': 50, 'orderby': None}
    for _o, _dft  in _opt_params.items():
        if _o not in params:
            params[_o] = _dft
    _prods = Product.query('product_uuid', **params)
    return jsonify({
        'status': 'OK',
        'products': _prods
        })

@mod.route("/by/source", methods=['GET'])
def get_bysource():
    """ Endpoint to fetch `Product`s by source's.
    """
    logger.info("Query Product by source...")
    params = request.args.to_dict()
    logger.debug(params)
    # Validate required params
    _needed_params = {'keys'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Complement optional params, and set default if needed
    _opt_params = {'cols': '', 'p':1, 'ipp': 50}
    for _o, _dft  in _opt_params.items():
        if _o not in params:
            params[_o] = _dft
    _prods = Product.query('source', **params)
    return jsonify({
        'status': 'OK',
        'products': _prods
        })


@mod.route("/count/by/source", methods=['GET'])
def get_countbysource():
    """ Endpoint to fetch `Product`s by source's.
    """
    logger.info("Query count Product by source...")
    params = request.args.to_dict()
    logger.debug(params)
    # Validate required params
    _needed_params = {'keys'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Complement optional params, and set default if needed
    _opt_params = {'all': False}
    for _o, _dft  in _opt_params.items():
        if _o not in params:
            params[_o] = _dft
    resp = {
        'status': 'OK'
    }
    resp.update(Product.query_count('source', **params))
    logger.info(resp)
    return jsonify(resp)


@mod.route("/matching/by/source", methods=['GET'])
def get_matchbysource():
    """ Endpoint to fetch `Product`s by source's.
    """
    logger.info("Query count Product by source...")
    params = request.args.to_dict()
    logger.debug(params)
    # Validate required params
    _needed_params = {'keys'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Complement optional params, and set default if needed
    _opt_params = {'cols': '', 'p':1, 'ipp': 50, 'all': '0'}
    for _o, _dft  in _opt_params.items():
        if _o not in params:
            params[_o] = _dft
    _prods = Product.query_match('source', **params)
    return jsonify({
        'status': 'OK',
        'products': _prods
        })
    


@mod.route("/by/attr", methods=['GET'])
def get_byattr():
    """ Endpoint to fetch `Product`s by attr's.
    """
    logger.info("Query Product by attr...")
    params = request.args.to_dict()
    logger.debug(params)
    # Validate required params
    _needed_params = {'keys'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Complement optional params, and set default if needed
    _opt_params = {'cols': '', 'p':1, 'ipp': 50,
        'rets': '', 'vals': ''}
    for _o, _dft  in _opt_params.items():
        if _o not in params:
            params[_o] = _dft
    _prods = Product.filter('attr.key',
        'product_attr.value', **params)
    return jsonify({
        'status': 'OK',
        'products': _prods
        })


@mod.route("/delete", methods=['GET'])
def delete_prod():
    """ Endpoint to delete a `Product`s by product_uuid.
    """
    logger.info("Delete Product...")
    params = request.args
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'uuid'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to delete Item
    _resp = Product.delete(params['uuid'])
    return jsonify({
        "status": "OK",
        "message": _resp['message']
        })


@mod.route("/delete/attr", methods=['GET'])
def delete_prod_attr():
    """ Endpoint to delete `Product`s attribute by Prod Attr ID
    """
    logger.info("Delete Product Attr...")
    params = request.args
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'uuid','id'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to delete Product extra
    _resp = Product.delete_extra(params['uuid'], params['id'], 'product_attr')
    return jsonify({
        "status": "OK",
        "message": _resp['message']
        })


@mod.route("/delete/image", methods=['GET'])
def delete_prod_img():
    """ Endpoint to delete `Product`s image by  Prod Image ID
    """
    logger.info("Delete Product Image...")
    params = request.args
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'uuid','id'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to delete Product extra
    _resp = Product.delete_extra(params['uuid'], params['id'], 'product_image')
    return jsonify({
        "status": "OK",
        "message": _resp['message']
        })


@mod.route("/normalized", methods=["POST"])
def upload_normalized():
    """ Endpoint to upsert normalized names to
        normalized table.
    """
    logger.info('Upload Normalized Names of products.')
    if not request.files:
        raise errors.ApiError(70007, "Missing file, add attachement!")
    if 'normalized.csv' not in request.files:
        raise errors.ApiError(70007, "Missing file name, add attachement!")
    if 'append' in request.args:
        _mode = 'append'
    else:
        _mode = 'replace'
    resp = Product.upload_normalized(request.files['normalized.csv'], _mode)
    return jsonify(resp)


@mod.route("/intersection", methods=['GET'])
def get_intersection():
    """ Endpoint to fetch `Product`s by attr's.

        /intersect?<field1>=<values>&<field2>=<values>
        translates to:
        where <field1> in (<vals>) and <field2> in (<vals>)

        @Request:
        - <field>=<values> : n number of fields and values to make the qry
        - cols : columns
        - p : page
        - ipp : items per page

        @Response:
        - products

    """
    logger.info("Query Product by attr...")
    params = dict(request.args)
    logger.debug(params)

    # The keys of the params are the fields
    if not params:
        logger.error(70001, "No params to query with")

    # Pagination default
    if not 'p' in params:
        params['p'] = 1
    if not 'ipp' in params:
        params['ipp'] = 100
        
    # Query items
    _prods = Product.intersection(**params)
        
    return jsonify({
        'status': 'OK',
        'products': _prods
        })

@mod.route("/reset", methods=["POST"])
def reset_match():
    """ Reset match by setting Item UUID to NULL
    """
    logger.info("Reseting product...")
    params = request.get_json()
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    if 'puuid' not in params:
        raise errors.ApiError(70001, "Missing required key params")
    # Call to update Product
    return jsonify(Product.undo_match(params['puuid']))