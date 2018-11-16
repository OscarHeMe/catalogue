# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.item import Item
from app import errors, logger
from flask import Response, stream_with_context
import json
from flask_cors import CORS, cross_origin

mod = Blueprint('item', __name__, url_prefix="/item")


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


@mod.route("/by/iuuid", methods=['GET'])
def get_byitem():
    """ Endpoint to fetch items by item_uuids
    """
    logger.info("Query Items by Item UUID...")
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
    _items = Item.query('item_uuid', **params)
    return jsonify({
        'status': 'OK',
        'items': _items
        })


@mod.route('/add', methods=['POST'])
def add_item():
    """ Endpoint to add a new `Item`
    """
    logger.info("Adding new Item...")
    params = request.get_json()
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'gtin', 'name', 'description'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to save Item
    _item = Item(params)
    _item.save()
    return jsonify({
        "status": "OK",
        "message": _item.message,
        "item_uuid": _item.item_uuid
    })


@mod.route('/modify', methods=['POST'])
def modify_item():
    """ Endpoint to modify a new `Item`
    """
    logger.info("Modify Item...")
    params = request.get_json()
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'item_uuid'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to save Item
    _item = Item(params)
    _item.save()
    return jsonify({
        "status": "OK",
        "message": _item.message,
        "item_uuid": _item.item_uuid
    })


@mod.route('/delete', methods=['GET'])
def delete_item():
    """ Endpoint to delete an `Item` by item_uuid
    """
    logger.info("Delete Item...")
    params = request.args
    logger.debug(params)
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'uuid'}
    if not _needed_params.issubset(params):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to delete Item
    _resp = Item.delete(params['uuid'])
    return jsonify({
        "status": "OK",
        "message": _resp['message']
    })


@mod.route('/details', methods=['GET'])
def details_item():
    """ Endpoint to get details for FrontEnd info
    """
    logger.info("Item details endpoint...")
    params = request.args
    # Validation
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    _needed_params = {'uuid'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Verify if item or product
    uuid_type = 'item_uuid'
    if not Item.exists({'item_uuid': params['uuid']}):
        uuid_type = 'product_uuid'
    _resp = Item.details(uuid_type, params['uuid'])
    logger.debug(_resp)
    return jsonify(_resp)
        

@mod.route('/details/info', methods=['GET'])
def details_info():
    """ Endpoint to get details of given items

        @Params:
            - values: <str> list of item_uuids comma separated
            - by: <str> field which the values are queried against
                (WHERE <by> = <value>)
            - cols: <str> can be item_uuid, gtin, name, description

        @Response:
            - resp: items list
    """
    logger.info("Info details")
    params = request.args
    # Validation
    if not params:
        raise errors.ApiError(70001, "Missing required key params")

    # Verify needed key-values
    _needed_params = {'values', 'by'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")

    if 'cols' not in params:
        cols = ['item_uuid', 'gtin', 'name', 'description']

    # Call to delete Item
    values = params['values'].split(",")
    by = params['by']
    _resp = Item.get(values, by=by, _cols=cols)
    return jsonify({
        "status": "OK",
        "message": _resp['message'],
        "items": _resp
    })


@mod.route('/elastic_items', methods=['POST'])
def elastic_items():
    """ Endpoint to get item details to fill the elasticsearch index
    @Params:
        - uuids: <str> uuids separated by comma
        - type: <str> item_uuid or product_uuid

    @Response:
        - resp: items list
    """
    logger.info("Getting details ES...")
    params = request.get_json()
    if not params:
        raise errors.ApiError(70001, "Missing required key params")
    # Verify needed key-values
    _needed_params = {'items', 'type'}
    if not _needed_params.issubset(params.keys()):
        raise errors.ApiError(70001, "Missing required key params")
    # Call to save Item
    _items = Item.get_elastic_items(params)
    return jsonify({
        "status": "OK",
        "message": "Those are the item details :D",
        "items": _items
    })

@mod.route('/catalogue_stream', methods=['GET'])
def generate_catalogue_stream():
    logger.info("Getting catalogue stream")
    params = request.args
    type_ = params.get("type", None)
    if type_ is not None and not (type_=='product_uuid' or type_=='item_uuid'):
        raise errors.ApiError(70001, "Type parameter is wrong: {}".format(type_))
    chunk_size = params.get("chunk_size", None)
    if chunk_size and chunk_size.isdigit():
        chunk_size = int(chunk_size)
    else:
        chunk_size = 1000
    total_items = Item.get_total_items(type_)
    def generate():
        yield '{"items": ['
        for offset_ in range(0, total_items, chunk_size):
            if (offset_ + chunk_size) >= total_items:
                yield json.dumps(Item.get_catalogue_uuids(type_, offset_=offset_, limit_=chunk_size))
            else:
                yield json.dumps(Item.get_catalogue_uuids(type_, offset_=offset_, limit_=chunk_size)) + ', '
        yield '], "message": "finished"}'

    return Response(stream_with_context(generate()), content_type='application/json')


@mod.route('/additional', methods=['GET'])
def vademecum_info():
    """ Endpoint to get info from vademecum
    """
    logger.info("Fetching Vademecum additonal info..")
    params = request.args
    if 'uuid' not in params:
        raise errors.ApiError(70001, "Missing required UUID param")
    # Call values
    _resp = Item.get_vademecum_info(params['uuid'])
    return jsonify(_resp)


@mod.route('/sitemap', methods=['GET'])
def sitemap():
    '''
    Getting items from scroll_df to create the sitemap

    '''
    # query text
    size_ = request.args.get('size', '100')
    from_ = request.args.get('from', '0')
    farma = request.args.get('farma', False)
    if not size_.isdigit():
        size_ = '100'

    if not from_.isdigit():
        from_ = '0'

    df = Item.get_sitemap_items(size_, from_, farma)
    items = df.to_dict(orient="records")
    if df.empty:
        logger.error("Df was empty!")
        return jsonify({'code': 'not found'}), 404

    response = {
        "items": items,
        "size": size_,
        "from": from_
    }
    # print response...
    return jsonify(response)


@mod.route('/filtered', methods=['POST'])
@cross_origin(origin="*")
def get_by_filters():
    """
        Get items given some filters
    """
    filters = request.get_json()
    print(filters)
    '''
    filters = [
        { "category" : "9406" },
        { "category" : "9352" },
        { "category" : "8865" },
        { "retailer" : "walmart" },
        { "retailer" : "superama" },
        { "retailer" : "ims" },
        { "item" : "67e8bc34-2e0d-460b-8ed0-72710b19f1b6" },
        { "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" }
    ]
    '''
    items = Item.get_by_filters(filters)
    return jsonify(items)
