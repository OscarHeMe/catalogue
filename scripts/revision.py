import os
import pandas as pd
from pygres import Pygres
from sqlalchemy import create_engine
from config import *
import sys

# DB Credentials
SQL_IDENTITY = os.getenv("SQL_IDENTITY", "identity.byprice.db") #"192.168.99.100")
SQL_ITEMS = os.getenv("SQL_ITEMS", "items.byprice.db") #"192.168.99.100")
SQL_IDENTITY_PORT = os.getenv("SQL_IDENTITY_PORT", 5432)
SQL_ITEMS_PORT = os.getenv("SQL_ITEMS_PORT", 5432)
M_SQL_USER = os.getenv("M_SQL_USER", "byprice")
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
    'identity', 'gtin', 'item_uuid,name,gtin', SQL_IDENTITY_PORT)
g_retailer = load_db(SQL_IDENTITY, M_SQL_USER, M_SQL_PASSWORD,
    'identity', 'gtin_retailer', 'item_uuid,item_id,retailer', SQL_IDENTITY_PORT)
print('Gtin Retailers:', len(g_retailer))

# Load Tables from  Items DB
i_retailer = load_db(SQL_ITEMS, M_SQL_USER, M_SQL_PASSWORD,
    'items', 'item_retailer', 'item_uuid,item_id,name,retailer,gtin', SQL_ITEMS_PORT)
print('Item Retailers:', len(i_retailer))

# Load Tables from  Catalogue DB
product = load_db(SQL_HOST, SQL_USER, SQL_PASSWORD,
    'catalogue', 'product', 'product_uuid,item_uuid,product_id,name,source,gtin')
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
    really_missing_cnt = 0
    for i, pin in p_inmigr.iterrows():
        _checked = product[(product.item_uuid==pin.item_uuid) &  (product.source==pin.source)]
        if _checked.empty:
            print('Looking for', pin)
            print('GTIN')
            _gt = gtin[(gtin.item_uuid == pin.item_uuid)]
            if not _gt.empty:
                print(_gt)
                input('This is recognized!')
            print('GTIN RETAILER')
            print(g_retailer[(g_retailer.item_uuid == pin.item_uuid) & (g_retailer.retailer == pin.source)])
            print('ITEM RETAILER')
            print(i_retailer[(i_retailer.item_uuid==pin.item_uuid) & (i_retailer.retailer==pin.source)])
            really_missing_cnt += 1
    print("Found {} products that are missing".format(really_missing_cnt))