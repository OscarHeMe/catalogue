import datetime
from flask import g
from app import errors, logger
from config import *
import requests
from pprint import pformat as pf
import ast
import json
from app.norm.normalize_text import key_format

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'

class Item(object):
    """
        Class perform Query methods on PostgreSQL items
    """

    @staticmethod
    def get_one():
        """
            Static Method to verify correct connection with Items Postgres DB
        """
        try:
            q = g._db.query("SELECT * FROM item LIMIT 1").fetch()
        except:
            logger.error("Postgres Items Connection error")
            return False
        for i in q:
            logger.info('Item UUID: ' + str(i['item_uuid']))
        return {'msg':'Postgres Items One Working!'}


    @staticmethod
    def get_categories(retailer="byprice"):
        """ Get list of categories from given retailer
        """
        rows = g._db.query("select * from category where retailer = %s",(retailer,)).fetch()
        print(rows)
        return rows or []


    @staticmethod
    def get_by_id(item_uuid, retailer=None):
        """
            Method to retrieve information from the most relevant retailer

            * Response format:
                {
                    'item_uuid' : 'cb4ad8b6ds4bsd6b4',
                    'name' : 'Medicamento',
                    'gtin': '75124225628898',
                    'description' : 'Medicamento que cura bien',
                    'images' : '"['https://imagen.byprice.com/byprice/medicamento']",
                    'retailer' : 'Walmart',
                    'retailer_key' : 'walmart',
                    'url': 'https://walmart.com.mx/medicamento/que/cura'
                }
        """
        try:
            it_q = g._db.query("""
                    SELECT i.gtin, i.name, i.description, ir.images, 
                    ir.url, r.name as retailer
                    FROM item i
                    INNER JOIN item_retailer ir
                    ON (ir.item_uuid = i.item_uuid)
                    INNER JOIN retailer r
                    ON (ir.retailer = r.key)
                    WHERE i.item_uuid = %s
                    """ + ( (""" AND ir.retailer = '%s' """%(retailer,)) if retailer else """ """ ) + """
                    ORDER BY hierarchy DESC
                    LIMIT 1
                """,(item_uuid,)).fetch()
            logger.debug(it_q)
            if not it_q:
                return False
            it_j = it_q[0]
            try:
                it_j['images'] = None if not it_j['images'] else ast.literal_eval(it_j['images'])
            except TypeError:
                logger.debug('No images..')
                it_j['images'] = ''
            it_j.update({'item_uuid':item_uuid, 'retailer_key':retailer})
            return it_j
        except Exception as e:
            logger.error(e)
            return False

    @staticmethod
    def retrieve_formatted_attribs(item_uuid):
        """
            Static Method to look for attributes of certain item 
            and return them in the correct format.

            * Response Format:

            [{
                'class_name' : str,
                'attr_name' : list,
                'attr_key' : list,
                'value': str
            },]
        """
        iattrs = []
        try:
            aq = g._db.query("""
                                SELECT ia.retailer, ia.value, a.key as attr_key, 
                                a.name as attr_name,
                                ac.name_es as class_name
                                FROM item_attribute ia
                                LEFT OUTER JOIN attribute a 
                                ON ia.id_attribute = a.id_attribute
                                LEFT OUTER JOIN attribute_class ac
                                ON a.id_attribute_class = ac.id_attribute_class
                                WHERE ia.item_uuid = '%s'
                                ORDER BY class_name
                            """%str(item_uuid)).fetch()
            #print('Attribute files')
            if len(aq) > 0:
                curr_cl = aq[0]['class_name']
                curr_atr = {
                                "class_name": curr_cl,
                                "attr_name" : [],
                                "attr_key" : [],
                                "value": aq[0]['value']
                            }
                for a in aq:
                    if a['class_name'] == curr_cl:
                        curr_atr['attr_name'].append(a['attr_name'])
                        curr_atr['attr_key'].append(a['attr_key'])
                    else:
                        iattrs.append(curr_atr)
                        curr_cl = a['class_name']
                        curr_atr = {
                                    "class_name": curr_cl,
                                    "attr_name" : [a['attr_name']],
                                    "attr_key" : [a['attr_key']],
                                    "value": a['value']
                                }
                iattrs.append(curr_atr)
                return iattrs
        except Exception as e:
            logger.error('Issues fetching attribs: '+str(e))
            logger.warning(str(item_uuid))
            return []
        return iattrs


    @staticmethod
    def retrieve_formatted_nutrs(item_uuid):
        """
            Static Method to look for nutrimental info of certain item 
            and return them in the correct format.

            * Response Format:

            [{                
                'class_name' : str,
                'attr_name' : list,
                'attr_key' : list,
                'value': str
            },]
        """
        nattrs = []
        try:
            aq = g._db.query("""
                                SELECT inu.value, a.key as attr_key, a.name as attr_name,
                                ac.name_es as class_name
                                FROM item_nutriment inu
                                LEFT OUTER JOIN item_nutriment_header inh
                                ON inh.id_nutriment_header = inu.nutriment_header
                                LEFT OUTER JOIN attribute a 
                                ON inu.id_attribute = a.id_attribute
                                LEFT OUTER JOIN attribute_class ac
                                ON a.id_attribute_class = ac.id_attribute_class
                                WHERE inu.item_uuid = '%s'
                            """%str(item_uuid)).fetch()
            #print('Nutriment files')
            if len(aq) > 0:
                curr_cl = aq[0]['class_name']
                curr_atr = {
                                "class_name": curr_cl,
                                "attr_name" : [],
                                "attr_key" : [],
                                "value": aq[0]['value']
                            }
                for a in aq:
                    if a['class_name'] == curr_cl:
                        curr_atr['attr_name'].append(a['attr_name'])
                        curr_atr['attr_key'].append(a['attr_key'])
                    else:
                        nattrs.append(curr_atr)
                        curr_cl = a['class_name']
                        curr_atr = {
                                    "class_name": curr_cl,
                                    "attr_name" : [a['attr_name']],
                                    "attr_key" : [a['attr_key']],
                                    "value": a['value']
                                }
                nattrs.append(curr_atr)
                return nattrs
        except Exception as e:
            logger.error('Issues fetching nutriments: '+str(e))
            logger.warning(str(item_uuid))
            return []
        return nattrs

    @staticmethod
    def retrieve_formatted_ingreds(item_uuid):
        """
            Static Method to look for Ingredients info of certain item 
            and return them in the correct format.

            * Response Format:

            [{
                'class_name' : str,
                'attr_name' : str,
                'attr_key' : str,
                'value': str
            },]
        """
        iattrs = []
        try:
            aq = g._db.query("""
                                SELECT ing.key as attr_key, ing.name as attr_name,
                                ing.retailer
                                FROM item_ingredient ii
                                LEFT OUTER JOIN ingredient ing
                                ON ing.id_ingredient = ii.id_ingredient
                                WHERE ii.item_uuid = '%s'
                            """%str(item_uuid)).fetch()
            #print('Ingredient files')
            if len(aq) > 0:
                curr_cl = 'Ingredientes'
                curr_atr = {
                                "class_name": curr_cl,
                                "attr_name" : [],
                                "attr_key" : [],
                                "value": None
                            }
                for a in aq:
                    curr_atr['attr_name'].append(a['attr_name'])
                    curr_atr['attr_key'].append(a['attr_key'])
                iattrs.append(curr_atr)
                return iattrs
        except Exception as e:
            logger.error('Issues fetching ingredients: '+str(e))
            logger.warning(str(item_uuid))
            return []
        return iattrs

    @staticmethod
    def retrieve_formatted_adds(item_uuid):
        """
            Static Method to look for Additional info of certain item 
            and return them in the correct format.

            * Response Format:

            [{
                'type' : str,
                'text' : str,
            },]
        """
        aattrs = []
        try:
            aq = g._db.query("""
                                SELECT iad.info_type as type, 
                                iad.description as txt
                                FROM item_additional iad
                                WHERE iad.item_uuid = '%s'
                            """%str(item_uuid)).fetch()
            #print('Ingredient files')
            for a in aq:
                print('ADDITIONAL')
                aattrs.append({
                                "type" : a['type'],
                                "text" : a['txt']
                            })
            return aattrs
        except Exception as e:
            logger.error('Issues fetching addditionals: '+str(e))
            logger.warning(str(item_uuid))
            return []
        return aattrs

    @staticmethod
    def filtered_cat(item_uuid):
        """ Method to Get Item info by UUID
        """
        try:
            it = g._db.query("""SELECT * FROM item 
                            WHERE item_uuid = '{}'
                            LIMIT 1""".format(item_uuid))\
                        .fetch()[0]
        except Exception as e:
            logger.error(e)
            logger.warning(str(item_uuid))
            return {
                'status':'ERROR',
                'msg': 'Could not find item.'
                }
        ex_info = Item.fetch_info(it['item_uuid'],[])
        frmt_item = {
                'retailers': ex_info['retailers'],
                'item_uuid': it['item_uuid'],
                'name' : it['name'],
                'names': ex_info['names'],
                'gtin' : it['gtin'],
                'images' : it['images'],
                'descriptions': ex_info['descriptions'],
                'date': str(it['last_modified']),
                'provider': ex_info['provider'],
                'brand': ex_info['brand'],
                'categories': ex_info['categories'],
                'attributes': ex_info['attributes'],
                'ingredients': ex_info['ingredients'],
                "additional" : ex_info['additional']
        }
        return {
                'status':'OK',
                'data': frmt_item
                }

    @staticmethod
    def fetch_info(item_uuid,categories=None):
        """
            Method to query all additional info of a product
        """        
        # Obtaining provider
        pq = g._db.query("""
                            SELECT * FROM provider
                            INNER JOIN item_provider ip
                            ON (ip.provider_uuid = provider.provider_uuid)
                            WHERE item_uuid = '%s'
                            LIMIT 1
                        """%str(item_uuid)).fetch()
        if len(pq) > 0:
            logger.debug(pf(pq[0]))
            prov = {
                        'name': pq[0]['name'],
                        'key' : pq[0]['key']
                    }
        else:
            logger.debug('It does not have provider registered!')
            prov = {'name':'','key':''}
        # Obtaining brand
        bq = g._db.query("""
                            SELECT * FROM brand
                            INNER JOIN item_brand ib
                            ON (ib.brand_uuid = brand.brand_uuid)
                            WHERE item_uuid = '%s'
                            LIMIT 1
                        """%str(item_uuid)).fetch()
        if len(bq) > 0:
            logger.debug(pf(bq[0]))
            brnd = {
                        'name': bq[0]['name'],
                        'key' : bq[0]['key']
                    }
        else:
            brnd = {
                        'name': '',
                        'key' : ''
                    }
        
        # Get only byprice categories
        try:
            cq = g._db.query("""
                                SELECT category.* FROM category
                                INNER JOIN item_category ic
                                ON (ic.id_category = category.id_category)
                                WHERE ic.item_uuid = '%s' 
                                """ %(str(item_uuid),)+ ("AND ic.id_category IN ("+ ",".join([ str(idc) for idc in categories ])+")" if categories != None and len(categories) > 0 else ""  )+"""
                                ORDER BY category.retailer
                            """).fetch()
        except Exception as e:
            logger.error('Issues fetching categs: '+str(e))
            logger.warning(str(item_uuid))
            cq = []
            

        categ = []
        retcat = []
        if len(cq) > 0:
            pret = cq[0]['retailer']
        for c in cq:
            if c['retailer'] == pret:
                retcat.append(c['name'])
            else:
                categ.append(retcat)
                pret = c['retailer']
                retcat = [c['name']]
            logger.debug(pf(c))
        categ.append(retcat)

        # Obtaining info from different retailers
        try:
            rqs = g._db.query("""
                            SELECT ir.*, r.hierarchy 
                            FROM item_retailer ir
                            INNER JOIN retailer r
                            ON (r.key = ir.retailer)
                            WHERE ir.item_uuid = '%s'
                            """%(str(item_uuid))).fetch()
        except Exception as e:
            logger.error('Issues fetching retailers: '+str(e))
            logger.warning(str(item_uuid))
            rqs = []

        rets, names, descriptions, images, ings = [], [], [], [], []
        if len(rqs) > 0:
            for rq in rqs:
                rets.append(rq['retailer'])
                names.append(rq['name'])
                descriptions.append(rq['description'])
                try:
                    img = ast.literal_eval(rq['images'])
                except:
                    img = [rq['images']]
                images = images + (img if img else [])
                ings = ings + ([rq['ingredients']] if rq['ingredients'] else [])
        # Ingredients list
        """
        # Omitir por el momento, tomar solo texto
        attrs_ings = Item.retrieve_formatted_ingreds(item_uuid)        
        if len(attrs_ings) > 0:
            ings = ings + [ ia['attr_name'][0] for ia in attrs_ings if ia['attr_name'][0] not in ings]
        """

        #Obtaining attributes (nutrimental, ingredients and additional)
        attribs = [] + Item.retrieve_formatted_attribs(item_uuid)
        attribs += Item.retrieve_formatted_nutrs(item_uuid)
        #attribs += attrs_ings   # Agregar para tomer en cuenta lista de ingredient attributes

        # Obtaining all additional info from item
        addits = Item.retrieve_formatted_adds(item_uuid)
        return {
                "names": names,
                "retailers" : rets,
                "descriptions" : descriptions,
                "brand": brnd,
                "categories": categ,
                "provider": prov,
                "attributes": attribs,
                "ingredients" : ings,
                "additional": addits,
                "images" : images
                }


    @staticmethod
    def get_catalogue(p, ipp, catalogue='byprice'):
        """
            Method to get all items, with a paginator option and will return such item
            with all existant categories, attributes, etc.
                p:          page
                ipp:        items per page
                catalogue:  retailer from which to retrieve the categories from
        """
        # Read valid Categories
        vcfile = open('data/valid_categories.json', 'r').read()
        valid_c = json.loads(vcfile)['categories']
        norm_vc = [key_format(x) for x in valid_c]
        if p < 1:
            p =  1
        try:
            # Get category ids
            categories = []
            if catalogue != None:
                rows = g._db.query("select id_category from category where retailer = %s",(catalogue,)).fetch()
                if len(rows) > 0:
                    categories = [ cat['id_category'] for cat in rows ]

            # Just get the items that are in the
            q = g._db.query("""
                SELECT *FROM item 
                OFFSET %s LIMIT %s
            """,( (p-1)*ipp, ipp)).fetch()                            
            to_fetch = False
        except Exception as e:
            logger.error("Issues fetching data from DB: " + str(e))
            return {'status': 'ERROR', 'msg': 'Issues fetching data from DB: '+str(e)}
        ## Loop over elements to retrieve all attributes, categories, nutriments an additional info
        frmt_items = []
        for it in q:
            ex_info = Item.fetch_info(it['item_uuid'],categories)
            frmt_items.append({
                        'retailers': ex_info['retailers'],
                        'item_uuid': it['item_uuid'],
                        'name' : it['name'],
                        'names': ex_info['names'],
                        'gtin' : it['gtin'],
                        'images' : it['images'],
                        'descriptions': ex_info['descriptions'],
                        'date': str(it['last_modified']),
                        'provider': ex_info['provider'],
                        'brand': ex_info['brand'],
                        'categories': ex_info['categories'],
                        'attributes': ex_info['attributes'],
                        'ingredients': ex_info['ingredients'],
                        "additional" : ex_info['additional']
                })
        return {'status':'OK', 'data':{'items':frmt_items, 'missing_count': to_fetch}}

    @staticmethod
    def list_gs1(p):
        """ Method to obtain all items from GS1"""
        try:
            lgs1 = g._db.query("SELECT item_uuid, name, gtin FROM item_retailer WHERE retailer = 'gs1' OFFSET {} LIMIT 50".format(p)).fetch()
            if not lgs1:
                raise  Exception('Empty List in Item Retailer!')
        except Exception as e:
            logger.error(e)
            return []        
        return lgs1

    
    @staticmethod
    def get_details(item_uuid=None, item_id=None, retailer=None):
        ''' Get item details given the uuid or (id and retailer)
        '''
        item = {}
        rows = g._db.query("select * from item where item_uuid in (select item_uuid from item_retailer where retailer = %s and item_id = %s)",(retailer, item_id)).fetch()
        if rows and len(rows) > 0:
            item=rows[0]
        return item

    @staticmethod
    def get_items_by_gtin(gtins):
        """ Get Items UUIDs from gtins list
        """
        if len(gtins) == 1:
            gtins = str(tuple(gtins)).replace(',','')
        else:
            gtins = str(tuple(gtins))
        try:
            iqry = """SELECT gtin, item_uuid 
                FROM item WHERE gtin IN {}""".format(gtins)
            logger.debug(iqry)
            items = g._db.query(iqry).fetch()
            return items
        except Exception as e:
            logger.error(e)
            return []

    @staticmethod
    def get_by_retailer(retailer, fields=["*"], categories=None):
        ''' Get items from given retailer
        '''
        if categories:
            q_categories = """ and item_uuid in ( select item_uuid from item_category where id_category in ("""+ """,""".join(categories) +""") )  """  
        q_fields = ", ".join(fields)
        rows = g._db.query("""
            select """+q_fields+""" from item_retailer 
            where retailer = %s 
            """ + ( q_categories if categories else """ """  ) + """
        """,(retailer,)).fetch()
        if not rows:
            return []
        return rows

    @staticmethod
    def get_by_filters(filters):
        ''' Get catalogue by filters: item, category, retailer, providers
        '''
        c = []
        r = []
        i = []
        p = []
        b = []
        ing = []
        # Build query
        for obj in filters:
            [(f, v)] = obj.items()
            if f == 'category':
                c.append(v)
            elif f == 'retailer':
                r.append("""'"""+v+"""'""")
            elif f == 'item':
                i.append("""'"""+v+"""'""")
            elif f == 'provider':
                p.append("""'"""+v+"""'""")
            elif f == 'brand':
                b.append("""'"""+v+"""'""")
            elif f == 'ingredient':
                ing.append("""'"""+v+"""'""")
        # Q by filters
        q = []
        if len(c) > 0:
            print(""", """.join(c) )
            q.append(""" item_uuid in (select item_uuid from item_category where id_category in ( """ + """, """.join(c) + """ )) """)
        if len(r) > 0:
            q.append(""" item_uuid in (select item_uuid from item_retailer where retailer in ( """ + """, """.join(r) + """ )) """)
        if len(p) > 0:
            q.append(""" item_uuid in (select item_uuid from item_provider where provider_uuid in ( """ + """, """.join(p) + """ )) """)
        if len(b) > 0:
            q.append(""" item_uuid in (select item_uuid from item_brand where brand_uuid in ( """ + """, """.join(b) + """ )) """)
        if len(ing) > 0:
            q.append(""" item_uuid in (select item_uuid from item_ingredient where id_ingredient in ( """ + """, """.join(ing) + """ )) """)
        if len(i) > 0:
            q.append(""" item_uuid in (""" + """, """.join(i) + """ ) """)
        where = (""" where """ + """ and """.join(q) ) if len(q) > 0 else """ limit 100 """
        # Build query
        qry= """ select item_uuid, name, gtin from item """ + where 
        items = g._db.query(qry).fetch()
        if not items:
            return []
        return items
