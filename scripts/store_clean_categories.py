from clean_category import get_categories_related
from clean_category import get_id_categories
from pygres import Pygres
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
from itertools import repeat
from datetime import datetime
import os

def print_(string):
    print(str(datetime.today())[:19] + "\t" + string)


def get_category_raw(*args):
    categories_raw = args[0][0]
    name = args[0][1]
    uuid = args[0][2]
    if categories_raw:
        categories = get_categories_related(categories_raw, min_bad_score=90)
    else:
        categories = False
    if not categories:
        categories = get_categories_related(name, min_bad_score=85, is_name=True)
    return {"categories": categories, "uuid": uuid}


def store_clean_categories(product_uuids=False, is_update=False):
    print_('connecting to database....')
    db = Pygres(
        {
            "SQL_HOST": os.getenv("SQL_HOST"),
            "SQL_PORT": os.getenv("SQL_PORT"),
            "SQL_DB": os.getenv("SQL_DB"),
            "SQL_USER": os.getenv("SQL_USER"),
            "SQL_PASSWORD": os.getenv("SQL_PASSWORD")
        }
    )
    conn = db.conn

    print_('Obtaining products')
    qry = """
        SELECT p.name as product_name, p.categories as categories_raw, i.item_uuid, p.product_uuid, i.name item_name
        FROM public.product p
        left outer join public.item i on i.item_uuid=p.item_uuid;
    """
    prods = pd.read_sql(qry, conn)
    if product_uuids:
        if isinstance(product_uuids, list):
            prods = prods[prods.product_uuid.isin(product_uuids)]
        else:
            print("Prodcut uuids arg should be a list of product uuids (strings)")
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
        print_("Creating csv items...")
        item_categories.to_csv('df_items.csv')
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
        print_("Creating products csv...")
        prods_product.to_csv('df_products.csv')
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
    print_("Creating csv final")
    prods.to_csv('final_products.csv')

    global id_categories
    id_categories = get_id_categories()

    print_("Inserting {} in product category".format(len(prods)))

    with Pool(4) as pool:
        pool.map(store_category_in_db, zip(prods.categories, prods.product_uuid, repeat(is_update)))
    db.close()
    print_("The script has finished!")


def main():
    store_clean_categories()


def store_category_in_db(*args):
    global id_categories
    db = Pygres(
        {
            "SQL_HOST": os.getenv("SQL_HOST"),
            "SQL_PORT": os.getenv("SQL_PORT"),
            "SQL_DB": os.getenv("SQL_DB"),
            "SQL_USER": os.getenv("SQL_USER"),
            "SQL_PASSWORD": os.getenv("SQL_PASSWORD")
        }
    )
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
        repeted_categories = list(prods_stored[prods_stored.category_name.isin(categories)].category_name)
        categories = [category for category in categories if category not in repeted_categories]
        prods = tuple(prods_stored[~prods_stored.category_name.isin(categories)].id_product_category)
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