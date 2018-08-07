import os
import datetime
import sys
import pandas as pd
from pygres import Pygres
from sqlalchemy import create_engine
from config import *
from tqdm import tqdm

# DB Credentials
SQL_IDENTITY = os.getenv("SQL_IDENTITY", "identity.byprice.db") #"192.168.99.100")
SQL_ITEMS = os.getenv("SQL_ITEMS", "items.byprice.db") #"192.168.99.100")
SQL_IDENTITY_PORT = os.getenv("SQL_IDENTITY_PORT", 5432)
SQL_ITEMS_PORT = os.getenv("SQL_ITEMS_PORT", 5432)
M_SQL_USER = os.getenv("M_SQL_USER", "byprice")
M_SQL_PASSWORD = os.getenv("M_SQL_PASSWORD", "")
# DB names
ITEMS_DB = 'items_dev'
IDENTITY_DB = 'identity_dev'
CATALOGUE_DB = 'catalogue_dev'


def load_db(host, user, psswd, db, table, cols, port=5432):
    _db = Pygres({
        'SQL_HOST': host, 'SQL_PORT': port, 'SQL_USER': user,
        'SQL_PASSWORD': psswd, 'SQL_DB': db
    })
    df = pd.read_sql("SELECT {} FROM {}".format(cols, table), _db.conn)
    _db.close()
    return df

# Load Tables from  Identity DB
gtin = load_db(SQL_IDENTITY, M_SQL_USER, M_SQL_PASSWORD,
    IDENTITY_DB, 'gtin', '*', SQL_IDENTITY_PORT)
g_retailer = load_db(SQL_IDENTITY, M_SQL_USER, M_SQL_PASSWORD,
    IDENTITY_DB, 'gtin_retailer', 'item_uuid,item_id,retailer', SQL_IDENTITY_PORT)
print('Gtin Retailers:', len(g_retailer))

# Load Tables from  Items DB
i_retailer = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
    ITEMS_DB, 'item_retailer', '*', SQL_ITEMS_PORT)
print('Item Retailers:', len(i_retailer))

# Load Tables from  Catalogue DB
item = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
    CATALOGUE_DB, 'item', 'item_uuid,name,gtin')
product = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
    CATALOGUE_DB, 'product', 'product_uuid,item_uuid,product_id,name,source,gtin,last_modified')
print('Current Products:', len(product))

# JOIN past tables (gtin_retailer + item_retailer = product)
past_product = pd.merge(g_retailer, i_retailer,
    on=['item_uuid', 'retailer'], how='outer')
print('Past Products: ', len(past_product))

# -------------------
# Missing ByPrice items
if len(sys.argv) > 1 and sys.argv[1] == 'missing_items':
    # Compute those Items that are in Past products but not in New Products
    _item_ret_set = set(past_product.groupby(['item_uuid', 'retailer']).indices.keys() \
        - product.groupby(['item_uuid', 'source']).indices.keys())
    print('Missing {} products to migrate!'.format(len(_item_ret_set)))
    _missing = pd.DataFrame(list(_item_ret_set), columns=['item_uuid', 'source'])
    _missing = _missing[~_missing.source.isin(['mara', 'nielsen', 'gs1', 'byprice'])]
    print('Missing {} products to migrate from needed sources!'.format(len(_missing)))
    # Acquire parameters of missing elements
    _missing = pd.merge(_missing, past_product.rename(columns={'retailer': 'source'}),
        on=['item_uuid', 'source'], how='left')
    _missing['item_id'] = _missing.item_id_x.combine_first(_missing.item_id_y)
    del _missing['item_id_x']
    del _missing['item_id_y']
    # Add name from GTIN
    _missing = pd.merge(_missing, gtin, on=['item_uuid'], how='left')
    _missing['gtin'] = _missing.gtin_y.combine_first(_missing.gtin_x)
    _missing['name'] = _missing.name_y.combine_first(_missing.name_x)
    del _missing['gtin_x'], _missing['name_x']
    del _missing['gtin_y'], _missing['name_y']
    print('Missing {} products with name!'.format(_missing['name'].dropna().count()))
    print('Missing {} products with gtin!'.format(_missing['gtin'].dropna().count()))
    # Insert into Catalogue DB
    if len(_missing) >= 0:
        _conn =  create_engine("postgresql://{}:{}@{}:{}/{}"
                                .format(SQL_USER, SQL_PASSWORD,
                                        SQL_HOST, SQL_PORT,
                                        SQL_DB))
        _missing['name'] = _missing['name'].apply(lambda x: str(x).strip().replace(',',''))
        _missing.rename(columns={'item_id': 'product_id'})\
            .set_index(['item_uuid', 'source'])\
            .to_sql('product', _conn,
                    if_exists='append',
                    chunksize=2000)
        print('Finished Loading Revision Products to Catalogue')
    del gtin, g_retailer, i_retailer, past_product, _missing

# -------------------
# Missing ByPrice categories
if len(sys.argv) > 1 and sys.argv[1] == 'missing_categs':
    # Load Product categories
    i_category = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        'items', 'item_category', '*')
    category = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        'items', 'category', '*')
    print('Item category:', len(i_category))
    print('Category:', len(category))
    complete_cat = pd.merge(i_category, category[category.retailer == 'byprice'],
        on='id_category', how='inner')
    print('Filled categories:', len(complete_cat))

    # Complete missing columns for product_category
    complete_prod_cat = pd.merge(
            product[['item_uuid', 'product_uuid']],
            complete_cat[['item_uuid', 'id_category', 'last_modified']],
            on=['item_uuid'], how='inner')\
        .drop(['item_uuid'], axis=1)\
        .set_index(['product_uuid'])

    # Load into DB
    _conn =  create_engine("postgresql://{}:{}@{}:{}/{}"
                                .format(SQL_USER, SQL_PASSWORD,
                                        SQL_HOST, SQL_PORT,
                                        SQL_DB))
    if not list(_conn.execute("""SELECT EXISTS (
                SELECT 1 FROM product_category pc
                INNER JOIN category c ON (c.id_category = pc.id_category)
                WHERE c.source = 'byprice' LIMIT 1
                )"""))[0]['exists']:
        print('Loading Byprice product categories')
        complete_prod_cat\
            .to_sql('product_category', _conn,
                if_exists='append', chunksize=2000)
        print('Finished Loading Revision Product Categories to Catalogue')

# -------------------
# Missing products found from migration
if len(sys.argv) > 1 and sys.argv[1] == 'products_not_in_migration':
    # Read File
    p_inmigr = pd.read_csv("data/dumps/missing_items.csv")
    p_inmigr.drop_duplicates(inplace=True)
    print('Verifying Missing products in Items and Identity DB')
    generated_items, generated_prods = [], []
    for i, pin in p_inmigr.iterrows():
        _checked = product[(product.item_uuid==pin.item_uuid) &  (product.source==pin.source)]
        if _checked.empty:
            print('Looking for', pin)
            _gt = gtin[(gtin.item_uuid == pin.item_uuid)]
            _gtr = g_retailer[(g_retailer.item_uuid == pin.item_uuid) & (g_retailer.retailer == pin.source)]
            _itr = i_retailer[(i_retailer.item_uuid==pin.item_uuid) & (i_retailer.retailer==pin.source)]
            _cit = item[(item.item_uuid == pin.item_uuid)]
            # There is a GTIN but no Catalogue ITEM, then create Catalogue Item records
            if not _gt.empty and _cit.empty:
                tmp_gt = _gt.to_dict(orient='records')[0]
                tmp_gt.update({
                    'description': tmp_gt['name'],
                    'checksum' : int(tmp_gt['checksum']),
                    'last_modified': tmp_gt['date']
                })
                del tmp_gt['gtin_14'], tmp_gt['gtin_13'], tmp_gt['gtin_12']
                del tmp_gt['gtin_8'], tmp_gt['date']
                generated_items.append(tmp_gt)
                print('Added GTIN to generate..')
            # If there is info in Gtin retailer and Item retailer, take it to reproduce it
            if not _itr.empty or not _gtr.empty:
                # Use Gtin retailer info
                _tmppr = _gtr.to_dict(orient='records')[0]
                _tmppr.update({
                    'product_id': _tmppr['item_id'],
                    'source': _tmppr['retailer']})
                del _tmppr['item_id'], _tmppr['retailer']
                # Use item retailer info
                tmp_itr = _itr.to_dict(orient='records')[0]
                _tmppr.update({
                    'name': tmp_itr['name'],
                    'description': tmp_itr['description'],
                    'categories': tmp_itr['categories'],
                    'url': tmp_itr['url'],
                    'brand': tmp_itr['brand'],
                    'provider': tmp_itr['provider'],
                    'ingredients': tmp_itr['ingredients'],
                    'last_modified': str(datetime.datetime.utcnow())
                })
                generated_prods.append(_tmppr)
                print('Added Product with Prev info')
            else:
                print('Not enough info to create product!')
                input("Issues....")
    print("Found {} items that were missing".format(len(generated_items)))
    print("Found {} products that were missing".format(len(generated_prods)))
    # Insert into Catalogue DB
    _conn =  create_engine("postgresql://{}:{}@{}:{}/{}"
                                .format(SQL_USER, SQL_PASSWORD,
                                        SQL_HOST, SQL_PORT,
                                        SQL_DB))
    df_gen_items = pd.DataFrame(generated_items)
    df_gen_items.set_index(['item_uuid'])\
            .to_sql('item', _conn,
                    if_exists='append',
                    chunksize=2000)
    print('Inserted Items!!')
    df_gen_prods = pd.DataFrame(generated_prods)
    df_gen_prods.set_index(['item_uuid', 'source'])\
            .to_sql('product', _conn,
                    if_exists='append',
                    chunksize=2000)
    print('Inserted Products!!')
    print('Finished updating products missing from file!')

# -------------------
# Missing ByPrice ingredients
if len(sys.argv) > 1 and sys.argv[1] == 'missing_ingreds':
    # Load Product ingredients
    i_ingredient = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        ITEMS_DB, 'item_ingredient', '*')
    ingredient = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        ITEMS_DB, 'ingredient', '*')
    print('Item ingredient:', len(i_ingredient))
    print('Ingredient:', len(ingredient[ingredient.retailer == 'byprice']))
    complete_ingred = pd.merge(i_ingredient, ingredient[ingredient.retailer == 'byprice'],
        on='id_ingredient', how='left')
    print('Filled ingredients:', len(complete_ingred))

    # Verify Clss and Attr in BYprice
    _clss  = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
        CATALOGUE_DB, 'clss', '*')
    _attr = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
        CATALOGUE_DB, 'attr', '*')
    # If no Ingredient clss existant
    if 'ingredient' not in _clss[_clss.source == 'byprice']['key'].tolist():
        _db = Pygres({
            'SQL_HOST': SQL_HOST, 'SQL_PORT': SQL_PORT, 'SQL_USER': SQL_USER,
            'SQL_PASSWORD': SQL_PASSWORD, 'SQL_DB': SQL_DB
        })
        _ingclss = _db.model('clss', 'id_clss')
        _ingclss.name = 'Ingredient'
        _ingclss.name_es = 'Ingrediente'
        _ingclss.description = 'Ingrediente'
        _ingclss.key = 'ingredient'
        _ingclss.source = 'byprice'
        _ingclss.save()
        bp_ing_clss = _ingclss.last_id
        print('Created Ingredient ByPrice Class')
        _db.close()
    else:
        bp_ing_clss = _clss[(_clss.key == 'ingredient') & (_clss.source == 'byprice')].id_clss.tolist()[0]
    ##
    # Catalogue Ingredients
    catalogue_ingreds = []
    bp_attrs_list = _attr[(_attr.source == 'byprice')]['key'].tolist()
    _db = Pygres({
            'SQL_HOST': SQL_HOST, 'SQL_PORT': SQL_PORT, 'SQL_USER': SQL_USER,
            'SQL_PASSWORD': SQL_PASSWORD, 'SQL_DB': SQL_DB
        })
    # For each Byprice ingredient verify if exists or needs to be created
    for j, bing in ingredient[ingredient.retailer == 'byprice'].iterrows():
        if bing.key not in bp_attrs_list:
            # Create Ingredient in catalogue
            _ingat = _db.model('attr', 'id_attr')
            _ingat.name = str(bing['name']).capitalize()
            _ingat.id_clss = bp_ing_clss
            _ingat.has_value = 0
            _ingat.key = bing.key
            _ingat.source = 'byprice'
            _ingat.save()
            print('Created {} Ingredient in Catalogue'.format(bing['name']))
            catalogue_ingreds.append({'id_attr': _ingat.last_id, 'key': bing.key})
        else:
            # Fetch Ingredient ID from Attribute Table
            catalogue_ingreds.append({'id_attr': _attr[(_attr['source']=='byprice') & (_attr['key'] == bing.key)].id_attr.tolist()[0],
                'key': bing.key})
            print('{} Ingredient already in Catalogue'.format(bing['name']))
    _db.close()
    print('Total catalogue ingredients', len(catalogue_ingreds))

    # Generate Catalogue Item Attribute for ingredients
    complete_ingred_item = pd.merge(
            complete_ingred[['item_uuid', 'key']],
            pd.DataFrame(catalogue_ingreds),
            on='key',
            how='left')\
        .drop(['key'], axis=1)\
        .set_index(['item_uuid'])
    # Add necessary columns
    complete_ingred_item['value'] = None
    complete_ingred_item['precision'] = None
    complete_ingred_item['last_modified'] = str(datetime.datetime.utcnow())

    # Load into DB
    _conn =  create_engine("postgresql://{}:{}@{}:{}/{}"
                                .format(SQL_USER, SQL_PASSWORD,
                                        SQL_HOST, SQL_PORT,
                                        SQL_DB))
    if not list(_conn.execute("""SELECT EXISTS (
                SELECT 1 FROM item_attr iat
                INNER JOIN attr a ON (a.id_attr = iat.id_attr)
                WHERE a.source = 'byprice' LIMIT 1
                )"""))[0]['exists']:
        print('Loading Byprice Item Attributes (ingredients)')
        complete_ingred_item\
            .to_sql('item_attr', _conn,
                if_exists='append', chunksize=2000)
        print('Finished Loading Revision Item Attribute Ingredients to Catalogue')
    else:
        print('Item Attribute Ingredients Already to Catalogue')

# -------------------
# Missing ByPrice Brands
if len(sys.argv) > 1 and sys.argv[1] == 'missing_brands':
    # Load Product brand
    i_brand = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        ITEMS_DB, "item_brand WHERE brand_uuid IN (SELECT brand_uuid FROM brand WHERE retailer = 'byprice')", '*')
    del i_brand['retailer']
    brand = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        ITEMS_DB, "brand WHERE retailer = 'byprice'", '*')
    print('Item brand:', len(i_brand))
    print('Brand:', len(brand))
    complete_brand = pd.merge(i_brand, brand,
        on='brand_uuid', how='left')
    print('Filled brand:', len(complete_brand.key.dropna()))
    # Verify Attr in Byprice that are brands
    _attr = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
        CATALOGUE_DB, "attr WHERE id_clss IN (SELECT id_clss FROM clss WHERE source = 'byprice' AND key = 'brand')", '*')
    # Brand Attributes
    brand_attrs = pd.merge(_attr, brand, on='key', how='left').drop_duplicates('key')
    print('Brand Attributes', len(brand_attrs))
    print('Brands in Items and not in Catalogue', len(set(complete_brand.brand_uuid.tolist()) - set(brand_attrs.brand_uuid.tolist())))
    print('Item brands available from brands in Catalogue', len(complete_brand[
                complete_brand.brand_uuid.isin(brand_attrs.brand_uuid.tolist()) ]))
    print('Item brands available from brands not in Catalogue', len(complete_brand[~complete_brand.brand_uuid.isin(brand_attrs.brand_uuid.tolist())] ) )
    # Generate Catalogue Item Attribute for ingredients
    complete_brand['brand_uuid'] = complete_brand['brand_uuid'].astype(str)
    brand_attrs['brand_uuid'] = brand_attrs['brand_uuid'].astype(str)
    complete_brand_item = pd.merge(
            complete_brand[['item_uuid', 'brand_uuid']],
            brand_attrs,
            on='brand_uuid',
            how='inner')\
        .drop(['key'], axis=1)\
        .set_index(['item_uuid'])
    print('Item brands for Catalogue', len(complete_brand_item))    
    # Delete unnecessary columns
    del complete_brand_item['brand_uuid'], complete_brand_item['name_x']
    del complete_brand_item['id_clss'], complete_brand_item['meta']
    del complete_brand_item['name_y'], complete_brand_item['retailer']
    del complete_brand_item['logo'], complete_brand_item['has_value']
    del complete_brand_item['source'], complete_brand_item['match']
    # Add necessary columns
    complete_brand_item['value'] = None
    complete_brand_item['precision'] = None
    complete_brand_item['last_modified'] = str(datetime.datetime.utcnow())

    # Load into DB
    _conn =  create_engine("postgresql://{}:{}@{}:{}/{}"
                                .format(SQL_USER, SQL_PASSWORD,
                                        SQL_HOST, SQL_PORT,
                                        SQL_DB))
    if not list(_conn.execute("""SELECT EXISTS (
                SELECT 1 FROM item_attr iat
                INNER JOIN attr a ON (a.id_attr = iat.id_attr)
                INNER JOIN clss c ON (c.id_clss = a.id_clss)
                WHERE a.source = 'byprice'
                AND c.key = 'brand' LIMIT 1
                )"""))[0]['exists']:
        print('Loading Byprice Item Attributes (brands)')
        complete_brand_item\
            .to_sql('item_attr', _conn,
                if_exists='append', chunksize=2000)
        print('Finished Loading Revision Item Attribute Brands to Catalogue')
    else:
        print('Item Attribute Brands Already to Catalogue')

# -------------------
# Wrong Sanborns IDs
if len(sys.argv) > 1 and sys.argv[1] == 'wrong_sanborns':
    print('Product')
    prods = product[product.source == 'sanborns']\
        [['item_uuid', 'product_uuid', 'name', 'product_id', 'gtin', 'source', 'last_modified']]\
        .copy()
    # Verify number of products
    print('Prods', len(prods))
    print('Prods with UUID', prods[~prods.item_uuid.isnull()].product_uuid.value_counts().count())
    # Aux elements
    to_update = []  # Store product_uuid to update and replacement_product_id
    to_delete = [] # Store product_uuid to delete
    not_found = [] # Store not found product_ids, gtins
    # Group by gtin
    for p, df in tqdm(prods.groupby('gtin')):
        if len(df) > 1:
            try:
                _matched = df[~df.item_uuid.isnull()].to_dict(orient='records')[0]
                not_matched = df[df.item_uuid.isnull()].to_dict(orient='records')[0]
            except Exception as e:
                try:
                    if df.item_uuid.value_counts().count() == 1:
                        _matched = df.sort_values(by='last_modified', ascending=False).to_dict(orient='records')[0]
                        not_matched = df.sort_values(by='last_modified', ascending=False).to_dict(orient='records')[-1]
                    else:
                        raise Exception(e)
                except Exception as e:
                    print(e)
                    print(df)
                    import sys 
                    sys.exit()
            # Store Product UUID matched, and the product_id from the other
            to_update.append({'product_uuid': _matched['product_uuid'], 'product_id': not_matched['product_id']})
            # Store Product UUID with no matched elements
            to_delete.append({'product_uuid': not_matched['product_uuid'], 'new_product_uuid': _matched['product_uuid']})
        else:
            _tmp = df.to_dict(orient='records')[0]
            not_found.append({'product_id': _tmp['product_id'],
                'product_uuid': _tmp['product_uuid'],
                'item_uuid': _tmp['item_uuid'],
                'gtin': _tmp['gtin']})
    # Update all values
    _db = Pygres({
            'SQL_HOST': SQL_HOST, 'SQL_PORT': SQL_PORT, 'SQL_USER': SQL_USER,
            'SQL_PASSWORD': SQL_PASSWORD, 'SQL_DB': SQL_DB
        })
    updated_ids = []
    for k in tqdm(to_update, desc='Updated elements'):
        try:
            _m = _db.model('product', 'product_uuid')
            _m.product_uuid = k['product_uuid']
            _m.product_id = k['product_id']
            _m.save()
            updated_ids.append(_m.last_id)
        except Exception as e:
            print(e)
    print('Updated {} products'.format(len(updated_ids)))
    # Update product attributes and product categories
    for j in tqdm(to_delete, desc='Updating Categs and Attrs'):
        try:
            _db.query("UPDATE product_attr SET product_uuid = '{}' WHERE product_uuid = '{}' "\
                    .format(j['new_product_uuid'], j['product_uuid']))
            _db.query("UPDATE product_category SET product_uuid = '{}' WHERE product_uuid = '{}' "\
                    .format(j['new_product_uuid'], j['product_uuid']))
            _db.query("UPDATE product_image SET product_uuid = '{}' WHERE product_uuid = '{}' "\
                    .format(j['new_product_uuid'], j['product_uuid']))
        except Exception as e:
            print(e)
    for k in tqdm(to_delete, desc='Deleted elements'):
        try:
            _db.query("DELETE FROM product where product_uuid = '{}'".format(k['product_uuid']))
        except Exception as e:
            print(e)
    _db.close()
    print('Finished updating Sanborns IDs!')
    
    
