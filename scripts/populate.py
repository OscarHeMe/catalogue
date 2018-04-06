import json
import app.utils.db as db
import os
from pprint import pprint
import datetime
import ast

# Retailer types
source_types = {
    'plm': 'data_provider',
    'mara': 'data_provider',
    'gs1': 'data_provider',
    'ims': 'data_provider',
    'byprice': 'master'
}

# Try to initialize DB
try:
    db.initdb()
except Exception as e:
    print(str(e))

# DB Connector and models
print('Connecting to Catalogue PSQL....')
_db = db.getdb()
print('Connected to Catalogue PSQL!')
msrc = _db.model("source","key")
matcls = _db.model("clss","id_clss")
mit = _db.model("item","item_uuid")
mcat = _db.model("category","id_category")
mat = _db.model("attr","id_attr")
mpr = _db.model("product","product_uuid")
mprcat = _db.model("product_category","id_product_category")
mprat = _db.model("product_attr","id_product_attr")
mii = _db.model("product_image","id_product_image")


def save_attr_classes(obj):
    """ Upsert `Clss` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM clss WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Clss already in DB!')
        return True
    # Load model
    if 'id_attribute_class' in obj:
        matcls.id_clss = obj['id_attribute_class']
    matcls.name = obj['name']
    matcls.name_es = obj['name_es']
    matcls.match = obj['match']
    matcls.key = obj['key']
    if 'description' in obj:
        matcls.description = obj['description']
    if 'retailer' in obj:
        matcls.source = obj['retailer']
    try:
        matcls.save()
        print('Saved clss:', matcls.last_id)
        if 'id_attribute_class' in obj:
            update_clss_seq()
        return True
    except Exception as e:
        print(e)
        return False

def update_attr_seq():
    """ Update attr.id_attr PSQL sequence to avoid issues
    """ 
    _seq = _db.query("""SELECT id_attr FROM attr 
        ORDER BY id_attr DESC LIMIT 1""").fetch()
    if not _seq:
        return False
    _db.query("ALTER SEQUENCE attr_id_attr_seq RESTART WITH {}"\
        .format(_seq[0]['id_attr'] + 1))
    return True

def update_clss_seq():
    """ Update clss.id_clss PSQL sequence to avoid issues
    """ 
    _seq = _db.query("""SELECT id_clss FROM clss 
        ORDER BY id_clss DESC LIMIT 1""").fetch()
    if not _seq:
        return False
    _db.query("ALTER SEQUENCE clss_id_clss_seq RESTART WITH {}"\
        .format(_seq[0]['id_clss'] + 1))
    return True

def save_attrs(obj):
    """ Upsert `Attr` Table
    """
    if 'retailer' in obj:
        _exists  = _db\
            .query("""SELECT EXISTS (SELECT 1 FROM attr 
            WHERE key = '{}' AND source = '{}')"""\
                .format(obj['key'], obj['retailer'])).fetch()
    else:
        _exists  = _db\
            .query("SELECT EXISTS (SELECT 1 FROM attr WHERE key = '{}')"\
                .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Attr already in DB!')
        return True
    # Load model
    if 'id_attribute' in obj:
        mat.id_attr = obj['id_attribute']
    mat.id_clss = obj['id_attribute_class']
    mat.name = obj['name']
    mat.key = obj['key']
    mat.match = obj['match']
    mat.has_value = 1 if obj['has_value'] else 0
    if 'meta' in obj:
        mat.source = obj['meta']
    if 'retailer' in obj:
        mat.source = obj['retailer']
    try:
        mat.save()
        print('Saved attr:', mat.last_id)
        if 'id_attribute' in obj:
            update_attr_seq()
        return mat.last_id
    except Exception as e:
        print(e)
        return False


def save_source(obj):
    """ Upsert `Source` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM source WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Source already in DB!')
        return True
    # Load model
    msrc.key = obj['key']
    msrc.name = obj['name']
    msrc.logo = obj['logo']
    msrc.type = source_types[obj['key']] \
        if obj['key'] in source_types else 'retailer'
    msrc.retailer = 0 if obj['key'] in source_types else 1
    msrc.hierarchy = obj['hierarchy']
    try:
        msrc.save()
        return True
    except Exception as e:
        print(e)
        return False

def save_gtin(obj):
    """ Upsert `Gtin` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM item WHERE item_uuid = '{}')"\
            .format(obj['item_uuid'])).fetch()
    if _exists[0]['exists']:
        print('Item already in DB!')
        return True
    # Loading model
    mit.item_uuid = str(obj['item_uuid'])
    mit.gtin = str(obj['gtin']).zfill(14)[-14:]
    if obj['checksum'] :
        mit.checksum = int(obj['checksum'])
    mit.name = obj['name']
    if 'description' in obj:
        mit.description = str(obj['description'])
    mit.last_modified = obj['date'] if 'date' in obj else obj['last_modified']
    try:
        mit.save()
        print('Saved item:', mit.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def garanty_parent(id_cat):
    """ Verify if parent exists otherwise create provisional empty record
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM category WHERE id_category = {})"\
            .format(id_cat)).fetch()
    if _exists[0]['exists']:
        print('Parent OK!')
        return True
    mcat.id_category = id_cat
    try:
        mcat.save()
        print('Created provisional parent:', mcat.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def save_category(obj):
    """ Upsert `Category` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM category WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Category already in DB!')
        return True
    # Loading model
    if obj['id_parent'] and obj['id_parent'] != 0:
        garanty_parent(obj['id_parent'])
        mcat.id_parent = obj['id_parent']
    mcat.id_category = obj['id_category']
    mcat.source = obj['retailer']
    mcat.name = obj['name']
    mcat.key = obj['key']
    mcat.code = obj['code']
    try:
        mcat.save()
        print('Saved category:', mcat.last_id)
        return True
    except Exception as e:
        print(e)
        return False


def save_product(obj):
    """ Upsert `Product` Table
    """
    _qry = """SELECT EXISTS 
            (SELECT 1 FROM product WHERE product_id = '{}'
            AND source = '{}')"""\
            .format(obj['item_id'], obj['retailer'])
    _exists  = _db.query(_qry).fetch()
    if _exists[0]['exists']:
        print('Product already in DB!')
        return False
    # Loading model
    mpr.item_uuid = obj['item_uuid']
    mpr.source = obj['retailer']
    mpr.product_id = obj['item_id']
    mpr.name = obj['name']
    mpr.gtin = obj['gtin']
    if 'description' in obj and obj['description']:
        mpr.description = obj['description']
    if 'raw_item' in obj:
        try:
            _raw_prod = json.loads(obj['raw_item'])
            mpr.raw_product = json.dumps(_raw_prod)
        except:
            pass
    if 'categories' in obj and obj['categories']:
        mpr.categories = str(obj['categories'])
    if 'ingredients' in obj and obj['ingredients']:
        mpr.ingredients = str(obj['ingredients'])
    if 'brand' in obj and obj['brand']:
        mpr.brand = str(obj['brand'])
    if 'provider' in obj and obj['provider']:
        mpr.provider = str(obj['provider'])
    if 'url' in obj and obj['url']:
        mpr.url = str(obj['url'])
    if 'images' in obj and obj['images']:
        mpr.images = str(obj['images'])
    mpr.last_modified = str(obj['last_modified'])
    try:
        mpr.save()
        print('Saved product:', mpr.last_id)
        return mpr.last_id
    except Exception as e:
        print(e)
        return False

def save_prod_categ(p_uuid, obj):
    """ Upsert `Product_category` Table
    """
    _qry = """SELECT id_category FROM category WHERE key = '{}'
            AND source = '{}'"""\
            .format(obj['attr_key'], obj['source'])
    _cat_id  = _db.query(_qry).fetch()
    if not _cat_id:
        print('Category not in DB!')
        return False
    _qry = """SELECT EXISTS ( SELECT 1 FROM product_category
             WHERE product_uuid = '{}' AND id_category = {})"""\
            .format(p_uuid, _cat_id[0]['id_category'])
    _exists = _db.query(_qry).fetch()
    if _exists[0]['exists']:
        print('Product Category already in DB!')
        return True
    # Load model
    mprcat.id_category = _cat_id[0]['id_category']
    mprcat.product_uuid = p_uuid
    mprcat.last_modified = obj['last_modified'] \
        if 'last_modified' in obj \
            else str(datetime.datetime.utcnow())
    try:
        mprcat.save()
        print('Saved product category:', mprcat.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def check_clss(obj):
    """ Verify `Clss` Table records
    """
    _qry = """SELECT id_clss FROM clss WHERE key = '{}' LIMIT 1"""\
            .format(obj['clss_key'])
    id_clss = _db.query(_qry).fetch()
    if id_clss:
        return id_clss[0]['id_clss']
    print('Class not in DB!')
    return None

def check_attr(obj, id_clss):
    """ Verify `Attr` Table records
    """
    _qry = """SELECT id_attr FROM attr 
        WHERE key = '{}' AND id_clss = {} AND source = '{}'
        LIMIT 1""".format(obj['attr_key'], id_clss, obj['source'])
    id_attr = _db.query(_qry).fetch()
    if not id_attr:
        id_attr = save_attrs({
            'key': obj['attr_key'],
            'name': obj['attr_name'],
            'retailer': obj['source'],
            'has_value': 1 if obj['value'] else 0,
            'id_attribute_class': id_clss,
            'match': None
            })
        if not id_attr:
            return None
    else:
        id_attr = id_attr[0]['id_attr']
    return id_attr

def save_prod_attr(obj):
    """ Upsert `Product_attr` Table
    """
    _qry = """SELECT EXISTS (SELECT 1 FROM product_attr 
            WHERE product_uuid = '{}' AND id_attr = '{}')"""\
            .format(obj['product_uuid'], obj['id_attr'])
    _exists  = _db.query(_qry).fetch()
    if _exists[0]['exists']:
        print('Product Attr already in DB!')
        return False
    mprat.id_attr = obj['id_attr']
    mprat.product_uuid = obj['product_uuid']
    mprat.value = obj['value']
    mprat.last_modified = obj['last_modified'] \
        if 'last_modified' in obj \
            else str(datetime.datetime.utcnow())
    if 'last_modified' in obj:
        mprat.precision = obj['precision']
    try:
        mprat.save()
        print('Saved product attr:', mprat.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def save_images(obj, p_uuid):
    """ Upsert `Product_image` Table
    """
    try:
        imgs = ast.literal_eval(obj['images'])
        assert isinstance(imgs, list)
    except:
        try:
            if not obj['images'] or str(obj['images']) == 'None':
                raise Exception('No images available')
            imgs = [str(obj['images'])]
        except:
            print('No images to load!')
            return False
    for _img in imgs:
        _qry = """SELECT EXISTS (SELECT 1 FROM product_image 
                WHERE product_uuid = '{}' AND image = '{}')"""\
                .format(p_uuid, _img)
        _exists  = _db.query(_qry).fetch()
        if _exists[0]['exists']:
            print('Product Image already in DB!')
            continue
        # Load model
        mii.product_uuid = p_uuid
        mii.image = str(_img)
        mii.last_modified = str(datetime.datetime.utcnow())
        try:
            mii.save()
            print('Saved product image:', mii.last_id)
        except Exception as e:
            print(e)
    return True

def save_items(items):
    """ Loop products and save all information
    """
    # Loop all items
    for gtin in items:
        # Save GTIN
        if not save_gtin(gtin):
            continue
        # Loop all products
        for prod in gtin['gtin_retailers']:
            if 'attributes' not in prod: # Delete this
                continue # Delete this
            if not prod['attributes']: ### Delete this
                continue ### Delete this
            # Save the product information
            _prod_uuid = save_product(prod)
            if not _prod_uuid:
                continue
            # Save all the attributes            
            if 'attributes' in prod and prod['attributes']:
                for _attr in prod['attributes']:
                    if _attr['clss_key'] == 'category':
                        # Save categories
                        save_prod_categ(_prod_uuid, _attr)
                    # Check the class / attribute
                    id_clss = check_clss(_attr)
                    if not id_clss:
                        continue
                    id_attr = check_attr(_attr, id_clss)
                    if not id_attr:
                        continue
                    # Save the attribute value
                    save_prod_attr({
                        "id_attr" : id_attr,
                        "product_uuid": _prod_uuid,
                        "value" : _attr['value'],
                    })
            pprint(prod)
            # Save the images
            save_images(prod, _prod_uuid)
            break
        if 'attributes' in prod and prod['attributes']: 
            break ### Temp break

                    

if __name__ == '__main__':
    """
        Start loop to read the files of the 
        entire catalogue
    """
    print("Starting populator!")
    # Source upload
    with open('data/dumps/retailers.json', 'r') as _fr:
        for _k, _r in json.loads(_fr.read()).items():
            print('Loading:',_k)
            # Save source
            save_source(_r)
    print('Saved Sources!')
    # Category upload
    with open('data/dumps/categories.json', 'r') as _fr:
        _tl = list([_r for _k, _r in json.loads(_fr.read()).items()])
        for _r in sorted(_tl,
                        key=lambda d: d['id_parent'] if d['id_parent'] else 0,
                        reverse=False):
            print('Loading:', _r['key'])
            # Save category
            save_category(_r)
    print('Saved Categories!')
    # Attribute Class upload
    with open('data/dumps/attribute_classes.json', 'r') as _fr:
        for _k, _r in json.loads(_fr.read()).items():
            print('Loading:',_k)
            # Save attribute classes
            save_attr_classes(_r)
    print('Saved Clsses!')
    # Upload Brand and Provider as  Attribute Class
    brand_prov = {
        "brand": {'name': 'Brand', 'name_es': 'Marca',
            'key': 'brand', 'match': None},
        "provider": {'name': 'Provider', 'name_es': 'Proveedor',
            'key': 'provider', 'match': None},
        "category": {'name': 'Category', 'name_es': 'Categoría',
            'key': 'category', 'match': None}
    }
    for _k, _r in brand_prov.items():
        print('Loading:', _k)
        # Save attribute classes
        save_attr_classes(_r)
    print('Saved Brand and Provider as Clsses!')
    # Attributes upload
    with open('data/dumps/attributes.json', 'r') as _fr:
        for _k, _r in json.loads(_fr.read()).items():
            print('Loading:',_k)
            # Save attributes
            save_attrs(_r)
    print('Saved Attrs!')
    page = 1
    catalogue_page = []
    # While there is a file, open it
    for _file in os.listdir("data/dumps/"):
        if 'catalogue' not in _file:
            continue
        page = "data/dumps/" + str(_file)
        print("Opening page: {}".format(page))
        with open(page) as fobj:
            catalogue_page = json.load(fobj)
            print('Found', len(catalogue_page), 'products in page!')
            save_items(catalogue_page)
    print("Finished saving catalogue!")
    # Close connector
    _db.close()
    