import os
import datetime
import sys
import pandas as pd
from pygres import Pygres
from sqlalchemy import create_engine
from config import *

# DB Credentials
SQL_IDENTITY = os.getenv("SQL_IDENTITY", "identity.byprice.db") #"192.168.99.100")
SQL_ITEMS = os.getenv("SQL_ITEMS", "items.byprice.db") #"192.168.99.100")
SQL_IDENTITY_PORT = os.getenv("SQL_IDENTITY_PORT", 5432)
SQL_ITEMS_PORT = os.getenv("SQL_ITEMS_PORT", 5432)
M_SQL_USER = os.getenv("M_SQL_USER", "postgres")
M_SQL_PASSWORD = os.getenv("M_SQL_PASSWORD", "")


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
    'identity_dev', 'gtin', '*', SQL_IDENTITY_PORT)
g_retailer = load_db(SQL_IDENTITY, M_SQL_USER, M_SQL_PASSWORD,
    'identity_dev', 'gtin_retailer', 'item_uuid,item_id,retailer', SQL_IDENTITY_PORT)
print('Gtin Retailers:', len(g_retailer))

# Load Tables from  Items DB
i_retailer = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
    'items_dev', 'item_retailer', '*', SQL_ITEMS_PORT)
print('Item Retailers:', len(i_retailer))

# Load Tables from  Catalogue DB
item = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
    'catalogue_dev', 'item', 'item_uuid,name,gtin')
product = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
    'catalogue_dev', 'product', 'product_uuid,item_uuid,product_id,name,source,gtin')
print('Current Products:', len(product))

# JOIN past tables (gtin_retailer + item_retailer = product)
past_product = pd.merge(g_retailer, i_retailer,
    on=['item_uuid', 'retailer'], how='outer')
print('Past Products: ', len(past_product))

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
        'items_dev', 'item_ingredient', '*')
    ingredient = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
        'items_dev', 'ingredient', '*')
    print('Item ingredient:', len(i_ingredient))
    print('Ingredient:', len(ingredient[ingredient.retailer == 'byprice']))
    complete_ingred = pd.merge(i_ingredient, ingredient[ingredient.retailer == 'byprice'],
        on='id_ingredient', how='left')
    print('Filled ingredients:', len(complete_ingred))
    print(complete_ingred.head())

    # Verify Clss and Attr in BYprice
    _clss = product = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
        'catalogue_dev', 'clss', '*')
    _attr = product = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
        'catalogue_dev', 'attr', '*')
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
        print('Created Ingredient ByPrice Class')
        _db.close()
    catalogue_ingreds = []
    # For each Byrpice ingredient verify if exists 
    import sys
    sys.exit()

    # Complete missing columns for item_attribute
    complete_prod_cat = pd.merge(
            product[['item_uuid']],
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