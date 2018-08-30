from clean_category import get_categories_related
from clean_category import get_id_categories
from pygres import Pygres
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
from datetime import datetime
import os
from sqlalchemy import create_engine
import numpy as np

def print_(string):
    print(str(datetime.today())[:19] + "\t" + string)


def connect_database(type_='pygres'):
    db_host = os.getenv("SQL_HOST")
    db_port = os.getenv("SQL_PORT")
    db_name = os.getenv("SQL_DB")
    db_user = os.getenv("SQL_USER")
    db_password = os.getenv("SQL_PASSWORD")
    if type_.lower() == 'pygres':
        return Pygres(
            {
                "SQL_HOST": db_host,
                "SQL_PORT": db_port,
                "SQL_DB": db_name,
                "SQL_USER": db_user,
                "SQL_PASSWORD": db_password
            }
        )
    if type_.lower() == 'sqlalchemy':
        return create_engine(
            'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'.format(
                db_user=db_user,
                db_password=db_password,
                db_host=db_host,
                db_port=db_port,
                db_name=db_name
            )
        )
    else:
        return False


def get_category_raw(*args):
    categories_raw = args[0][0]
    name = args[0][1]
    uuid = args[0][2]
    if categories_raw:
        categories = get_categories_related(categories_raw, min_bad_score=90)
    # if not categories:
    #     categories = get_categories_related(name, min_bad_score=85, is_name=True)
    else:
        categories = False
    return {"categories": categories, "uuid": uuid}


def deprecate_categories():
    db = connect_database()
    print_("Deprecating old categories")
    qry_deprecate = """
        UPDATE public.product_category
        SET deprecated=1
        WHERE id_category in (
            SELECT id_category 
                FROM category
                WHERE source='byprice' 
            );
    """
    db.query(qry_deprecate)


def store_clean_categories(product_uuids=False, is_update=False):
    db = connect_database()
    conn = db.conn

    print_('Obtaining products')
    qry = """
        SELECT p.name as product_name, p.categories as categories_raw, i.item_uuid, p.product_uuid, i.name item_name
        FROM public.product p
        left join public.item i on i.item_uuid=p.item_uuid;
    """
    prods = pd.read_sql(qry, conn)
    if product_uuids:
        if isinstance(product_uuids, list):
            prods = prods[prods.product_uuid.isin(product_uuids)]
        else:
            print("Product uuids arg should be a list of product uuids (strings)")
            return False

    print_('{} items obtained..'.format(len(prods)))
    db.close()
    item_uuids = []
    categories_raw = []
    names = []
    print_('Splitting data...')
    prods_item = prods[~prods.item_uuid.isnull()]
    prods_product = prods[prods.item_uuid.isnull()]
    print_('{} have item_uuid..'.format(len(prods_item)))
    print_('{} just have product_uuid..'.format(len(prods_product)))

    if not prods_item.empty:
        print_('Obtaining categories & names from items')
        for item_uuid, group_ in tqdm(prods_item.groupby('item_uuid')):
            item_uuids.append(item_uuid)
            categories_raw.append([str(cat) for cat in group_.categories_raw if cat])
            names.append([str(p_name) for p_name in group_.product_name if p_name])

        print_('Creatng items raw dataframe')
        item_categories = pd.DataFrame()
        item_categories['item_uuid'] = item_uuids
        item_categories['categories_raw'] = categories_raw
        item_categories['product_name'] = names

        print_('Obtaining {} clean categories from items raw'.format(len(item_categories)))
        with Pool(4) as pool:
            categories_clean_items = pool.map(get_category_raw, tuple(zip(categories_raw, names, item_uuids)))
        print_('Converting clean categories to dataframe...')
        df_item = pd.DataFrame(categories_clean_items)
        df_item.rename(columns={"uuid": "item_uuid"}, inplace=True)
        item_categories = item_categories.merge(df_item, on="item_uuid")
        print_("Creating pickle items...")
        item_categories.to_pickle('df_items.p')
        item_categories = item_categories[['item_uuid', 'categories']]

        prods_i = prods.merge(item_categories, on='item_uuid', how='inner')

    else:
        print("There are no item uuids")
        prods_i = pd.DataFrame()

    if not prods_product.empty:
        print_('Obtaining {} clean categories from products raw'.format(len(prods_product)))
        categories_raw = prods_product.categories_raw
        product_names = prods_product.product_name
        product_uuids = prods_product.product_uuid
        with Pool(4) as pool:
            categories_clean_prods = pool.map(get_category_raw, tuple(zip(categories_raw, product_names, product_uuids)))
        print_('Converting clean categories to dataframe...')
        df_prod = pd.DataFrame(categories_clean_prods)
        df_prod.rename(columns={"uuid": "product_uuid"}, inplace=True)
        prods_product = prods_product.merge(df_prod, on='product_uuid')
        print_("Creating products pickle...")
        prods_product.to_pickle('df_products.p')
        prods_product = prods_product[['product_uuid', 'categories']]

        prods_p = prods.merge(prods_product, on='product_uuid', how='inner')

    else:
        print("There are no product uuids")
        prods_p = pd.DataFrame()

    if not prods_i.empty and not prods_p.empty:
        prods = pd.concat([prods_i, prods_p])
    elif not prods_i.empty:
        prods = prods_i
    elif not prods_p.empty:
        prods = prods_p
    else:
        print("There are no items and no products to create categories")
        return False
    print_("Creating pickle final")
    prods.to_pickle('final_products.p')


    id_categories = get_id_categories()

    print_("Inserting {} in product category".format(len(prods)))

    prods = prods[['product_uuid', 'categories']]
    prods = prods[(prods.categories != False) & (prods.categories.str.len() > 0)]
    prods_categories = pd.DataFrame({'product_uuid': np.repeat(prods.product_uuid.values, prods.categories.str.len()),
                                     'category': np.concatenate(prods.categories.values)})
    prods_categories['id_category'] = prods_categories.category.apply(id_categories.get).astype(int)

    prods_categories.to_pickle("prods_categories.p")

    del(prods_categories['category'])
    engine = connect_database('sqlalchemy')
    prods_categories.to_sql('product_category', index=False, con=engine, if_exists='append', chunksize=2000)
    # with Pool(4) as pool:
    #     pool.map(store_category_in_db, zip(prods.categories, prods.product_uuid, repeat(is_update)))


    print_("The script has finished!")


def main():
    deprecate_categories()
    store_clean_categories()


def store_category_in_db(*args):
    global id_categories
    db = connect_database()
    model = db.model('product_category', 'id_product_category')
    categories = args[0][0]
    product_uuid = args[0][1]
    is_update = args[0][2]

    if is_update:
        prods_stored = pd.read_sql("""
            SELECT id_product_category, c.name category_name
                FROM product_category pc INNER JOIN category c on pc.id_category=c.id_category
                WHERE product_uuid = '{}' and source='byprice';
        """.format(product_uuid), db.conn)
        #print("Matched categories:", categories)
        repeated_categories = list(prods_stored[prods_stored.category_name.isin(categories)].category_name)
        #print("Repeated categories:", repeated_categories)
        categories = [category for category in categories if category not in repeated_categories]
        #print("Insert categories:", categories)
        prods = tuple(prods_stored[~prods_stored.category_name.isin(categories+repeated_categories)].id_product_category)
        #print("Delete categories:", list(prods_stored[~prods_stored.category_name.isin(categories)].category_name))
        if prods:
            prods = str(prods).replace(",)", ")")
            delete_query = """
                DELETE FROM product_category WHERE id_product_category in {}
            """.format(prods)
            print(delete_query)
            db.query(delete_query)

    for category in categories:
        model.id_category = id_categories.get(category)
        model.product_uuid = product_uuid
        model.save()
    db.close()


if __name__ == "__main__":
    main()