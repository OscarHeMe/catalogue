import app.utils.dbs as dbs
from pprint import pprint
import json

db_items = dbs.connect_psql_items()
db_identity = dbs.connect_psql_identity()

"""
    1. Query all gtins and items
    2. Query all gtin_retailers and item_retailers
    3. Query all attribtues and brands
"""

catalogue = []

def get_retailers():
    rets = db_items.query("select * from retailer ").fetch()
    retailers = { r['key'] : r for r in rets }
    return retailers

def get_categories():
    cats = db_items.query("select * from category").fetch()
    categs = { r['key'] : r for r in cats }
    return categs

def get_attr_classes():
    at_class = db_items.query("select * from attribute_class order by id_attribute_class desc ").fetch()
    at_class = { r['key'] : r for r in at_class }
    return at_class

def get_attributes():
    attr = db_items.query("select * from attribute order by id_attribute desc").fetch()
    attr = { r['key'] : r for r in attr }
    return attr

def get_products():
    print("Getting products...")
    # Query identity
    gtins = db_identity\
        .query("select * from gtin order by item_uuid limit 20")\
        .fetch()
    gtin_uuids = set([ g['item_uuid'] for g in gtins ]) 
    # Gtin retailers
    gtin_retailers = {}
    gtin_ret = db_identity\
        .query("select * from gtin_retailer order by item_uuid limit 10000 ")\
        .fetch()
    for gr in gtin_ret:
        gr['date'] = gr['date'].strftime("%Y-%m-%d")
        gr['date_matched'] = gr['date_matched'].strftime("%Y-%m-%d") if gr['date_matched'] else None
        if gr['item_uuid'] not in gtin_retailers:
            gtin_retailers[gr['item_uuid']] = []
        gtin_retailers[gr['item_uuid']].append(gr)
    for i,gtin in enumerate(gtins):
        print("GTIN: {}".format(i))
        # Get the item_retailers        
        if gtin['item_uuid'] in gtin_retailers:
            gtins[i]['gtin_retailers'] = gtin_retailers[gtin['item_uuid']]
        gtins[i]['date'] = gtin['date'].strftime("%Y-%m-%d")
    # All items to check if there are some that dont exists in gtin
    items = db_items.query("select item_uuid from item ").fetch()
    item_uuids = set([ i['item_uuid'] for i in items ])
    # Delete gitn_retailers
    del gtin_ret
    del gtin_retailers
    del items
    del item_uuids
    # Check items and gtins coincidences
    return gtins

    
def get_items(uuid, ret):
    # Query the item (retailer: byprice)
    #item = db_items.query("select * from item where item_uuid = %s",(uuid,)).fetch()
    #if not item:
    #return False
    #prod = item[0]
    #prod['last_modified'] = prod['last_modified'].strftime("%Y-%m-%d")
    #prod['retailer'] = 'byprice'

    # Get the item_retailers...
    group = []
    items = db_items.query("""select * from item_retailer
        where item_uuid = %s and retailer = %s""",(uuid,ret)).fetch()
    if not items:
        return {}
    for i, it in enumerate(items):
        # Get the attributes in general of the item
        attrs = []
        attributes = db_items.query("""
            select ac.name as clss_name, ac.key as clss_key, a.name as attr_name, a.key as attr_key, ia.value as value
            from item_attribute ia
            inner join attribute a on a.id_attribute = ia.id_attribute 
            inner join attribute_class ac on ac.id_attribute_class = a.id_attribute_class
            where ia.item_uuid = %s
            and retailer = %s
        """,(it['item_uuid'], it['retailer'])).fetch()
        if attributes:
            print('Attrs', attributes)
            for attr in attributes:
                attrs.append({
                    "clss_name" : attr['clss_name'],
                    "clss_key" : attr['clss_key'],
                    "attr_name" : attr['attr_name'],
                    "attr_key" : attr['attr_key'],
                    "value" : attr['value'],
                    "source" : it['retailer']
                })
        # Categories
        cats = db_items.query("""
            select c.name as name, c.key as key, c.code 
            from item_category ic
            inner join category c on ic.id_category = c.id_category
            where ic.item_uuid = %s
            and retailer = %s
        """,(it['item_uuid'], it['retailer'])).fetch()
        if cats:
            print('Categories', cats)
            for cat in cats:
                attrs.append({
                    'clss_name' : 'Categoría',
                    'clss_key' : 'category',
                    'attr_name' : cat['name'],
                    'attr_key' : cat['key'],
                    'source' : it['retailer'],
                    'value' : cat['code']
                })

        # Query all other info of the item
        brands = db_items.query("""
            select b.name as name, b.key as key
            from item_brand ib
            inner join brand b on b.brand_uuid = ib.brand_uuid
            where ib.item_uuid = %s 
            and b.retailer = %s
        """,(it['item_uuid'], it['retailer'])).fetch()
        if brands:
            print(brands)
            for brand in brands:
                attrs.append({
                    'clss_name' : 'Marca',
                    'clss_key' : 'brand',
                    'attr_name' : brand['name'],
                    'attr_key' : brand['key'],
                    'source' : it['retailer'],
                    'value' : ''
                })
        # Query all the providers
        provs = db_items.query("""
            select p.name as name, p.key as key
            from item_provider ip
            inner join provider p on p.provider_uuid = ip.provider_uuid
            where ip.item_uuid = %s 
            and p.retailer = %s
        """,(it['item_uuid'], it['retailer'])).fetch()
        if provs:
            for prov in provs:
                attrs.append({
                    'clss_name' : 'Proveedor',
                    'clss_key' : 'provider',
                    'attr_name' : prov['name'],
                    'attr_key' : prov['key'],
                    'source' : it['retailer'],
                    'value' : ''
                })
        # Query all ingredients
        ings = db_items.query("""
            select i.name as name, i.key as key
            from item_ingredient ii
            inner join ingredient i on i.id_ingredient = ii.id_ingredient
            where ii.item_uuid = %s 
            and i.retailer = %s
        """,(it['item_uuid'], it['retailer'])).fetch()
        if ings:
            for ing in ings:
                attrs.append({
                    'clss_name' : 'Ingrediente',
                    'clss_key' : 'ingredient',
                    'attr_name' : ing['name'],
                    'attr_key' : ing['key'],
                    'source' : it['retailer'],
                    'value' : ''
                })
        # Set the attributes of the item_retailer
        try:
            items[i]['last_modified'] = items[i]['last_modified'].strftime("%Y-%m-%d")
        except:
            pass
        if attrs:
            items[i]['attributes'] = [dict(y) for y in set(tuple(a.items()) for a in attrs)]
        else:
            items[i]['attributes'] = []
    return items[0]

def run():
    """ Main method
    """
    products = []
    products = get_products()
    # Retailers
    retailers = get_retailers()
    with open("data/dumps/retailers.json","w") as file:
        json.dump(retailers, file)
        print("Saved retailers")
    # Categories
    categs = get_categories()
    with open("data/dumps/categories.json","w") as file:
        json.dump(categs, file)
        print("Saved categories")
    # Attribute classes
    attr_classes = get_attr_classes()
    with open("data/dumps/attribute_classes.json","w") as file:
        json.dump(attr_classes, file)
        print("Saved attribute classes")
    # Attributes
    attrs = get_attributes()
    with open("data/dumps/attributes.json","w") as file:
        json.dump(attrs, file)
        print("Saved attributes")
    # Get all information of every retailer of every product
    page = 0
    catalogue_page = []
    for i, item in enumerate(products):
        print("Product: {}".format(i))
        if len(catalogue_page) >= 1000:
            page+=1
            with open("data/dumps/catalogue_"+str(page)+".json","w") as file:
                json.dump(catalogue_page, file)
                print("Saving catalogue...", str(page))
            catalogue_page = []
            ### temp break
            import sys
            sys.exit()
        prods = []
        if 'gtin_retailers' in item:
            for prod in item['gtin_retailers']:
                # Get item retailers
                _tmp = get_items(prod['item_uuid'], prod['retailer'])
                _tmp.update(prod)
                prods.append(_tmp)
            # Update vars
            item.update({'gtin_retailers': prods})
        # Store in catalogue page
        catalogue_page.append(item)
    # Save last loop
    with open("data/dumps/catalogue_"+str(page)+".json","w") as file:
        json.dump(catalogue_page, file)
        print("Saving catalogue...", str(page))

if __name__ == '__main__':
    run()