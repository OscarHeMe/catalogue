# -*- coding: utf-8 -*-
#import pandas as pd
#import numpy as np
import json
import datetime
import requests
import app.db as db
from pygres import Pygres
from config import *
from pprint import pformat as pf
import ast
import re
import app.norm.normalize_attributes as na
import app.norm.normalize_text as nt

# DB Instance
pdb = db.getdb()
db_iden = Pygres(dict(
        SQL_HOST = SQL_HOST,
        SQL_DB = 'identity',
        SQL_USER = SQL_USER,
        SQL_PASSWORD = SQL_PASSWORD, 
        SQL_PORT=SQL_PORT,
    ))


# Categories
types = ['families','classes', 'bricks']
# Open categories file
gs1_categ_file  = open('data/gs1_categories.json', 'r').read()
categ_json = json.loads(gs1_categ_file)

#Vars
prod_attrs_classes = set()
prod_attrs_params = set()

# Populate categories table
def categorize(data, parent=None, force=False):
    if not force:
        if len(pdb.query("""SELECT * FROM category 
                            WHERE retailer = 'gs1' 
                            AND retailer_reference = '%s'"""%(data[0]['id'])).fetch()) > 0:
            print('DB already populated!')
            return
    for c in data:
        cnam = c['name']
        print(c['name'], ':',cnam)
        fam_m = pdb.model('category', 'id_category')
        fam_m.id_parent = parent
        fam_m.name =  cnam
        fam_m.key = nt.key_format(cnam.replace('/',' '))
        fam_m.retailer = 'gs1'
        fam_m.retailer_reference = c['id']
        fam_m.save()
        l_parent = fam_m.last_id
        for ck in c.keys():
            if ck in types:
                ## Call to go deeper
                categorize(c[ck],l_parent, force)

def attributize(force=False):
    # Open attribute files
    at_class = json.loads(open('data/nutrimental_attributes_class.json').read())
    atc_model = pdb.model('attribute_class', 'id_attribute_class')        
    # Populate tables
    for atc in at_class:
        row = pdb.query("""SELECT * FROM attribute_class WHERE key = '%s'"""%(atc['key'],)).fetch()
        if len(row) > 0 and row != None:
            atc_model.id_attribute_class = row[0]['id_attribute_class']
        atc_model.name = atc['name']
        atc_model.name_es = atc['name_es']
        atc_model.key = atc['key']
        atc_model.match = atc['match']
        atc_model.description = atc['description']
        atc_model.save()
    print('Saved Attribute Classes')

    #Attribute file
    ats = json.loads(open('data/attributes.json').read())

    #Matchable classes
    matchable_classes = pdb.query("SELECT key,match FROM attribute_class").fetch()
    matchable_rel = {}
    for mc in matchable_classes:
        matchable_rel[mc['key']] = ast.literal_eval(mc['match'])

    # Matching to class
    all_rels = [x for y in matchable_rel.values() for x in y]
    atr = {}
    for at in ats.keys():
        if at in all_rels:
            for mr_k, mr in matchable_rel.items():
                if at in mr:
                    save_attributes(at, ats[at])

def save_attributes(attr_class, attrs_l):
    try:
        iac = pdb.query("SELECT * FROM attribute_class where key = '%s'"%(attr_class,)).fetch()[0]['id_attribute_class']
    except Exception as e:
        print(e)
        print("Could not fetch Attribute class by key")
        return False
    for a in attrs_l:
        a_model = pdb.model('attribute', 'id_attribute')
        a_model.id_attribute_class = iac
        a_model.name = a['name']
        a_model.key = a['key']
        a_model.match = a['match']
        a_model.has_value = a['has_value'] if 'has_value' in a.keys() else None
        a_model.save()
    return True
            
def verify_retailer():
    rq = pdb.query("SELECT * FROM retailer where key = 'gs1'").fetch()
    if len(rq) > 0:
        print('GS1 already created')
        return
    rmodel = pdb.model('retailer', 'key')
    rmodel.key = 'gs1'
    rmodel.name = 'GS1'
    rmodel.logo = 'gs1.png'
    rmodel.type = 'partner'
    rmodel.hierarchy = 1
    rmodel.save()
    print('GS1 record generated')


def generate_issues_report(issue, cause, gtin, item):
    """ Method to generate issues report"""
    print('Generating issues report.....')
    # Generate Report
    ##try:
    #    rep_df = pd.read_csv('parsed_data/issues_report.csv')
    #except:
        # Not existent file
    #    rep_df = pd.DataFrame()
    #print('Fetched CSV report file')
    try:
        #rep_df.append(
        #rep_df = pd.DataFrame([{'issue': str(issue), 
        #                   'cause': str(cause),
        #                   'gtin': str(gtin),
        #                   'item': str(item)}])
        #rep_df.to_csv('parsed_data/issues_report.csv', mode='a', header=False)
        # WILL GENERATE ISSUES REPORT
        pass
    except Exception as e:
        #print(rep_df)
        print('Issues writing file!!', str(e))
        return 
    print('Report Saved in File!')

def exists_retailer(ret):
    """ Retailer existance method """
    try:
        r = pdb.query("SELECT * FROM retailer WHERE key = '%s' LIMIT 1"%str(ret)).fetch()[0]['key']
    except Exception as e:
        print("Not able to fetch retailer from DB", str(e))
        return False
    return True

def upsert_brand(brand, ret):
    """ Brand revision or creation"""
    try:
        b = pdb.query("""SELECT * FROM brand 
                        WHERE key = '%s' 
                        LIMIT 1 """%str(brand).lower().replace(' ','_').replace("'","")).fetch()[0]['brand_uuid']
        return b
    except:
        print('Could not find brand in pdb, creating new...')
        try:
            bmodel = pdb.model('brand','brand_uuid')
            bmodel.name = str(brand).replace("'", "")
            bmodel.key = str(brand).lower().replace(' ', '_').replace("'", "")
            bmodel.retailer = ret
            bmodel.logo = str(brand).lower().replace(' ', '_').replace("'", "")+'.png'
            bmodel.save()
            return bmodel.last_id
        except Exception as e:
            print('Issues with: ', str(brand))
            print('Could not save new brand!', str(e))
            return ''
            

def upsert_provider(provider, ret):
    """ Provider revision or creation"""
    if provider is None or str(provider) == 'None':
        return ''
    try:
        p = pdb.query("""SELECT * FROM provider 
                        WHERE key = '%s' 
                        LIMIT 1 """%str(provider).lower().replace(' ','_').replace("'","")).fetch()[0]['provider_uuid']
        return p
    except:
        print('Could not find provider in pdb, creating new...')
        try:
            pmodel = pdb.model('provider','provider_uuid')
            pmodel.key = str(provider).lower().replace(' ', '_').replace("'","")
            pmodel.name = str(provider)
            pmodel.retailer = ret
            pmodel.logo = str(provider).lower().replace(' ', '_').replace("'","")+'.png'
            pmodel.save()
            return pmodel.last_id
        except Exception as e:
            print('Could not save new provider!', str(e))
            return ''

def upsert_item_identified(params):
    """ Identify items by gtin or generate new register """
    try:
        it_uuid = db_iden.query("""SELECT item_uuid FROM gtin WHERE 
                        to_number(gtin, '999999999999999') = %s 
                        OR gtin = '%s' 
                        OR gtin like '%s' 
                        LIMIT 1 """%(str(params['gtin']),
                                 str(int(params['gtin'])),
                                 '%%'+str(params['gtin'])+'%%')).fetch()
        if len(item_uuid) > 0:
            return it_uuid
    except:
        print("GTIN not found, generating new gtin record.")
    # If not found an existant record, generate a new one
    itmodel = db_iden.model('gtin', 'item_uuid')
    itmodel.gtin = str(params['gtin']).zfill(14) 
    itmodel.gtin_14 = str(params['gtin']).zfill(14)
    itmodel.name = params['name']
    itmodel.checksum = str(params['gtin'])[-1]
    itmodel.date = str(datetime.datetime.utcnow())
    itmodel.save()
    return itmodel.last_id

def upsert_item(item_uuid, provider_uuid, params):
    """ Method to save item in case it doesn't exist"""
    try:
        it = pdb.query("SELECT * FROM item where item_uuid = '%s'"%str(item_uuid)).fetch()
    except:
        print('Could not fetch information from db')
        return False
    if len(it) > 0:
        # If the item exists just return True to use same record
        return True
    else:
        # If it does not exist, create register
        try:
            imodel = pdb.model('item', 'item_uuid')
            imodel.item_uuid = item_uuid
            imodel.gtin = str(params['gtin']).zfill(14)
            imodel.name = str(params['name'])
            imodel.description = str(params['name'])
            imodel.images = str(params['images'])
            imodel.last_modified = str(datetime.datetime.utcnow())
            imodel.save()
            
            if provider_uuid != '' and provider_uuid is not None:
                pmodel = pdb.model('item_provider', 'id_item_provider')
                pmodel.item_uuid = item_uuid
                pmodel.provider_uuid = provider_uuid
                pmodel.last_modified = str(datetime.datetime.utcnow())
                pmodel.save()
        except Exception as e:
            print('Issues saving item... ', str(e))
            print(params['name'])
            print(item_uuid)
            print(provider_uuid)
            print(params['images'])
            return False
        return True

def check_atclass(attr_class, params):
    """ Method to verify existance of certain class or Generates new record """
    try:
        acq_str = """SELECT * FROM attribute_class 
                    WHERE match like '%s' 
                    LIMIT 1 """%('%%'+str(attr_class).lower().replace("'","").replace(' ','_')+'%%')
        cl = pdb.query(acq_str).fetch()
    except Exception as e:
        print('Could not fetch information from db', str(e))
        return False
    if len(cl) > 0:
        #If there is an existing record return id 
        return cl[0]['id_attribute_class']
    # If not, create new attribute class (NOT create right now, Generate report)
    generate_issues_report('Issues over Attribute class',
                          str(attr_class),
                          params['gtin'],
                          str(params)) 
    return False
    # change to the following later
    """
    try:
        cmodel = db.model('attribute_class', 'id_attribute_class')
        cmodel.name = str(attr_class)
        cmodel.name_es = str(attr_class)
        cmodel.match = str([str(attr_class).lower().replace(' ','_'),
                            str(attr_class).lower().replace(' ','')])
        cmodel.key = str(attr_class).lower().replace(' ','')
        cmodel.description = str(attr_class)
        cmodel.save()
        return cmodel.last_id
    except Exception as e:
        print('Issues saving attribute class...')
        return False
    """

def check_at(attr, ac_id):
    """ Method to verify existance of certain attribute or Generates new record """
    try:
        cl = pdb.query("""SELECT * FROM attribute
                        WHERE id_attribute_class = %d 
                        AND match LIKE '%s' 
                        LIMIT 1 """%(int(ac_id),'%%'+str(attr['attribute']).lower().replace("'","").replace(' ','_')+'%%')).fetch()
    except Exception as e:
        print('Could not fetch information from db', str(e))
        return False
    if len(cl) > 0:
        #If there is an existing record return id 
        return cl[0]['id_attribute']
    # If not, create new attribute class
    try:
        # Getting similar info from other attrs
        cl = pdb.query("""SELECT * FROM attribute
                        WHERE match LIKE '%s'
                        LIMIT 1"""%('%%'+str(attr['attribute']).lower().replace("'","").replace(' ','_')+'%%')).fetch()
        if len(cl) > 0:
            simcl = cl[0]
        else:
            simcl = {}
        cmodel = db.model('attribute', 'id_attribute')
        cmodel.id_attribute_class = ac_id
        cmodel.name = str(attr['attribute']) if 'name' not in simcl.keys() else simcl['name']
        cmodel.match = str([str(attr['attribute']).lower().replace(' ','_'),
                            str(attr['attribute']).lower().replace(' ','')]) \
                        if 'match' not in simcl.keys() else simcl['match']
        cmodel.key = str(attr['attribute']).lower().replace(' ','_') if 'key' not in simcl.keys() else simcl['key']
        cmodel.has_value = 1 if 'value' in attr.keys() else 0
        cmodel.save()
        return cmodel.last_id
    except Exception as e:
        print('Issues saving attribute class...')
        return False

def add_attributes(item_uuid, params):
    """ Verify attributes existance and generate item related records """
    print(params)
    for at in params['prod_attrs']:
        print('Getting attribute class...')
        at_class_id = check_atclass(at['class'], params)
        if not at_class_id:
            print('Attribute class not found!')
            return False#continue
        at_id = check_at(at, at_class_id)
        if not at_id:
            continue
        try:
            print('Saving item_attributes...')
            item_at = pdb.model('item_attribute', 'id_item_attribute')
            item_at.item_uuid = item_uuid
            item_at.id_attribute = at_id
            item_at.retailer = params['retailer']
            item_at.value = at['value'] if 'value' in at.keys() else None
            item_at.precision = at['precision'] if 'precision' in at.keys() else None
            item_at.last_modified = str(datetime.datetime.utcnow())
            item_at.save()
        except:
            print('Issues saving in DB')
            return False
    return True


def get_nutrition_head(item_uuid, ni):
    """ Method to et nutriment info id or generate a record"""
    
    q_str = """SELECT id_nutriment_header FROM item_nutriment_header 
                WHERE item_uuid = '%s' 
                AND serving_type ='%s' 
                AND serving_unit = '%s' """%(str(item_uuid),
                                           str(ni['serving_type']),
                                           str(ni['serving_unit']))
    if ('serving_value' in ni.keys()) and isinstance(ni['serving_value'], int) \
                                    and (ni['serving_value'] != None)\
                                    and (ni['serving_value'] != 'N/A'):
        q_str += 'AND serving_value = %s '%str(ni['serving_value'])
    print('About to fetch Nutrimental Header from DB.....')
    try:
        nh = db.query(q_str).fetch()
    except Exception as e:
        print('Could not fetch Nutrimental header:', str(e))
        print(q_str)
        nh = []
    print('Fetched Nutrimental Header from DB!')
    if (len(nh) > 0):
        return nh[0]['id_nutriment_header']
    else:
        try:
            nhmodel = pdb.model('item_nutriment_header', 'id_nutriment_header')
            nhmodel.item_uuid = str(item_uuid)
            nhmodel.serving_type = str(ni['serving_type'])
            nhmodel.serving_value = float(ni['serving_value']) if ('serving_value' in ni.keys()) and (ni['serving_value'] is not None) else None
            nhmodel.serving_unit = str(ni['serving_unit'])
            nhmodel.save()
            return nhmodel.last_id
        except Exception as e:
            print('Issues saving item nutriment header!', str(e))
            return False

def get_nutrition_attr(nat, params):
    """ Method to verify nutrimental attribute from class """
    try:
        #print('Getting NUtrimental attribute class...')
        nacl_id = pdb.query("""SELECT * FROM attribute_class 
                            WHERE match like '%s' 
                            LIMIT 1 """%('%%'+str(nat['class']).lower().replace(' ','_')+'%%')).fetch()
        #print('Got Nutrimental Attribute Class fetch!')
    except:
        print("Issues fetching from DB")
        return False
    if len(nacl_id) == 0:
        #print('Could not find Valid Nutrimental Class: ', str(nat))
        generate_issues_report('Issues over Nutrimental Attribute class', 
                                str(nat['class']), 
                                params['gtin'], 
                                str(params))
        return False
    try:
        # If found continue with the attribute
        nacl_id = nacl_id[0]['id_attribute_class']
        na = pdb.query("""SELECT * FROM attribute 
                        WHERE match like '%s' 
                        AND id_attribute_class = %s """%('%%'+str(nat['attribute']).lower().replace(' ','_')+'%%', 
                                                       str(nacl_id))).fetch()
        if len(na) == 0:
            ba = pdb.query("""SELECT * FROM attribute 
                        WHERE match like '%s' 
                        LIMIT 1 """%('%%'+str(nat['attribute']).lower().replace(' ','_')+'%%')).fetch()
            if len(ba) == 0:
                generate_issues_report('Issues over Nutrimental Attribute', 
                                        str(nat['attribute']), 
                                        params['gtin'], 
                                        str(params))
                try:
                    if nat['attribute'] is None:
                        nat['attribute'] = 'Contents'
                    ba = [{'name': nat['attribute'], 
                           'match':str([nat['attribute'].lower().replace(' ','_')]),
                           'key': nat['attribute'].lower().replace(' ','_')}]
                except:
                    print('Issues generating ba', str(nat))
            try:
                namodel = pdb.model('attribute', 'id_attribute')
                namodel.id_attribute_class = int(nacl_id)
                namodel.name = ba[0]['name']
                namodel.match = ba[0]['match']
                namodel.key = ba[0]['key']
                namodel.has_value = 1 if 'value' in nat.keys() else 0
                namodel.save()
            except:
                print('Issues saving the new attribute!!')
                return False
            na = [{'id_attribute': namodel.last_id}]
    except Exception as e:
        print('Could not get attribute id', str(e))
        return False
    return int(na[0]['id_attribute'])       
        
def add_nutrimental(item_uuid, nutr_info, params):
    """ Method to generate or verify nutritional headers, get nutritional (attribute classes) classes 
        and generate item nutriments records
    """
    for ni in nutr_info:
        print('Getting nutrimental header...')
        nh_id = get_nutrition_head(item_uuid, ni)
        print('Got nutrimental header!')
        for nat in ni['nutriments']:
            nat_id = get_nutrition_attr(nat, params)
            if not nat_id :
                print('Issues saving Nutrimental info! (attribute)')
                continue
            try:
                inmodel = pdb.model('item_nutriment', 'id_item_nutriment')
                inmodel.item_uuid = str(item_uuid)
                inmodel.id_attribute = int(nat_id)
                inmodel.retailer = params['retailer']
                inmodel.nutriment_header = int(nh_id)
                inmodel.value = nat['value'] if 'value' in nat.keys() else None
                inmodel.precision = nat['precision'] if 'precision' in nat.keys() else None
                inmodel.last_modified = str(datetime.datetime.utcnow())
                inmodel.save()
                print('Saved new item_nutriment')
            except Exception as e:
                print('Issues saving nutriment item record!', str(e))
                return False
    return True
                
def add_brand_item(item_uuid, brand_id, params):
    """ Method to add attach brand to item brand"""
    if brand_id == '':
        # NO existant brand
        return False
    try:
        ibmodel = pdb.model('item_brand', 'id_item_brand')
        ibmodel.item_uuid = str(item_uuid)
        ibmodel.brand_uuid = str(brand_id)
        ibmodel.retailer = params['retailer']
        ibmodel.last_modified = str(datetime.datetime.utcnow())
        ibmodel.save()
    except:
        print('Issues saving brand to item brand')
        return False
        
def add_category_item(item_uuid, cat_code, params):
    """ Method to fetch existant categories and save them to item category """
    cats = []
    cq = pdb.query("SELECT * FROM category WHERE retailer_reference = '%s' LIMIT 1"%str(cat_code)).fetch()
    parent = None
    if len(cq) > 0:
        cats.append(cq[0]['id_category'])
        parent = cq[0]['id_parent']
    while parent is not None:
        cq = pdb.query("SELECT * FROM category WHERE id_category = '%s' "%str(parent)).fetch()
        if len(cq) > 0:
            cats.append(cq[0]['id_category'])
            parent = cq[0]['id_parent']
        else:
            parent = None
    for cat in cats:
        try:
            cmodel = pdb.model('item_category', 'id_item_category')
            cmodel.item_uuid = item_uuid
            cmodel.id_category = cat
            cmodel.last_modified = str(datetime.datetime.utcnow())
            cmodel.save()
        except:
            print('Issues saving category, continue to the next...')
            continue
    return True

def add_ingredient_item(item_uuid, ingred_text, params):
    """ Method to save all ingredients """
    if ingred_text == '' or ingred_text is None:
        print('Ingredient empty!')
        return False
    ingreds = ingred_text.replace(' y ', ', ').split(',')
    for ing in ingreds:
        print('Ingredient: ', ing)
        try:
            iding = pdb.query("""SELECT id_ingredient FROM ingredient WHERE key = '%s' 
                            LIMIT 1 """%str(ing).lower().strip().replace(' ', '_').replace("'",'')).fetch()
        except:
            print('Issues parsing ingredient')
            if pdb.conn.closed == 1:
                print('###################### Issue')
            iding = []
        if len(iding) > 0:
            print('Item ingredient already found: ', iding)
            iding = iding[0]['id_ingredient']
        else:
            try:
                ingmodel = pdb.model('ingredient', 'id_ingredient')
                ingmodel.name = str(ing).strip().replace("'","")
                ingmodel.key = str(ing).lower().strip().replace(' ','_').replace("'","")
                ingmodel.retailer = params['retailer']
                ingmodel.save()    
                iding = ingmodel.last_id
            except:
                print('Issues creating new ingredient')
                continue
        print('Got Ingredient ID!')
        try:
            print('Saving item ingredient ', iding)
            itingmodel = pdb.model('item_ingredient', 'id_item_ingredient')
            itingmodel.item_uuid = str(item_uuid)
            itingmodel.id_ingredient = iding
            itingmodel.last_modified = str(datetime.datetime.utcnow())
            itingmodel.save()
            print('Saved Ingredient: ',str(itingmodel.last_id))
        except:
            print('Issues saving item ingredient')
            return False
    return True

def add_info(item_uuid, additional, params):
    """Method to save additional info, like instructions"""
    print('Saving additional....')
    for ad in additional:
        if 'instructions_type' not in ad.keys():
            print("Do not have instructions type: ",ad)
            continue
        if ad['instructions_type'] == 'allergen_code':
            # Do query for Allergy types for Allergen codes in Spanish
            desc = ' '.join([ad['content'], ad['allergy_type']])
        else:
            desc = ad['instructions']
        try:
            admodel = pdb.model('item_additional', 'id_item_additional')
            try:
                tmp_ad = pdb.query("""SELECT id_item_additional FROM item_additional 
                                WHERE item_uuid = '%s' 
                                AND info_type = '%s' """%(str(item_uuid), str(ad['instructions_type']))).fetch()
            except Exception as e:
                print('Issues fetching info type: ', str(e))
                tmp_ad = []
            if len(tmp_ad) > 0:
                admodel.id_item_additional = tmp_ad[0]['id_item_additional']
            admodel.item_uuid = str(item_uuid)
            admodel.info_type = str(ad['instructions_type']) if 'instructions_type'in ad.keys() else None
            admodel.retailer = params['retailer']
            admodel.description = str(desc)
            admodel.last_modified = str(datetime.datetime.utcnow())
            admodel.save()
        except:
            print('Could not save this additional info %s and %s'%(str(ad), str(desc)))
            if pdb.conn.closed == 1:
                pass
            continue
    return True

def add_item_retailer(item_uuid, params):
    """Method to generate Item Retailer"""
    try:
        tmp_ir = pdb.query("""SELECT id_item_retailer FROM item_retailer 
                        WHERE item_uuid = '%s' 
                        AND retailer = '%s' 
                        LIMIT 1 """%(str(item_uuid), params['retailer'])).fetch()
    except Exception as e:
        print('Issues searching existant item retailer: ', str(e))
        if pdb.conn.closed == 1:
            return False
    try:
        irmodel = pdb.model('item_retailer', 'id_item_retailer')
        if len(tmp_ir) > 0:
            irmodel.id_item_retailer = tmp_ir[0]['id_item_retailer']
        irmodel.item_uuid = str(item_uuid)
        irmodel.retailer = params['retailer']
        irmodel.gtin= params['gtin']
        irmodel.name = params['name']
        irmodel.description = params['description'] if 'description' in params.keys() else None
        irmodel.brand = params['brand'] if 'brand' in params.keys() else None
        irmodel.provider = params['provider'] if 'provider' in params.keys() else None
        irmodel.category = params['category_code']
        irmodel.attributes = str(params['prod_attrs'])
        irmodel.ingredients = str(params['ingreds'])
        irmodel.url = params['url'] if 'url' in params.keys() else None
        irmodel.images = str(params['images'])
        irmodel.last_modified = str(datetime.datetime.utcnow())
        irmodel.save()
    except Exception as e:
        print('Issues saving Item retailer: ', e)
        return False
    return True
         
def save_all_item(params):
    # Retailer verification
    if not exists_retailer(params['retailer']):
        print('Not available retailer, Upload retailer')
        return False
    print('Found retailer!')
    # Brand identification
    brand_id = upsert_brand(params['brand'], params['retailer'])
    if brand_id == '':
        print('Could not identify PROVIDER, continue...')
    else:
        print('Got Brand!')
    # Provider Identification
    provider_id = upsert_provider(params['provider'], params['retailer'])
    if provider_id == '':
        print('Could not identify PROVIDER, continue...')
    else:
        print('Got provider!')
    # Verify item_uuid and Save item
    item_uuid = upsert_item_identified(params)
    if not upsert_item(item_uuid, provider_id, params):
        print('Could not find or save the new item')
        return False
    print('Saved Item!')
    # Verify existent attributes, attribute classes and save item attributes
    if not add_attributes(item_uuid, params):
        print('Could not save attributes')
    else:
        print('Saved Attributes')
    # Generate nutrimental attributes
    if not add_nutrimental(item_uuid, params['nutr_attrs'], params):
        print('Could not save nutrimental info')
        return False
    print('Saved Nutrimental')
    # Attach brand to item brand
    if not add_brand_item(item_uuid, brand_id, params):
        print('Could not save brand to item_brand')
    else:
        print('Saved Item Brand')
    # Query and attach to item_brand
    if not add_category_item(item_uuid, params['category_code'], params):
        print('Could not save all categories')
    else:
        print('Saved Item Categories')
    # Generate ingredient record and save it into item ingredient
    if not add_ingredient_item(item_uuid, params['ingreds'], params):
        print('Could not save all ingredients')
    else:
        print('Saved Item Ingredient')
    # Save Additional info 
    if not add_info(item_uuid, params['additional'], params):
        print('Could not save additional info')
    else:
        print('Saved Additional')
    # Save item retailer
    if not add_item_retailer(item_uuid, params):
        print('Could not save Item Retailer')
        return False
    print('Saved Item Retailer')
    # Finished
    print('Item (%s) saved!'%(params['gtin']))
    return True

def list_dict_parser(objeto, search_key, key_param, position=0):
    """Method to return search key verifying if the object is list or dict"""
    item = None
    if search_key in objeto.keys():
        try:
            psl = objeto[search_key]
            if type(psl) is list:
                return psl[position][key_param]['value']
            elif type(psl) is dict:
                return psl[key_param]['value']
            else:
                return item
        except:
            pass
    return item

def list_dict_parser_selected(objeto, search_key, selected_key, selected_value):
    """Method to return search key verifying if the object is list or dict"""
    item = None
    if search_key in objeto.keys():
        psl = objeto[search_key]
        if type(psl) is list:
            for el in psl:
                if el[selected_key] == selected_value:
                    return el['value']
            return psl[position]['value']
        elif type(psl) is dict:
            return psl['value']
        else:
            return item
    return item

def nutrient_cast(det):
    """Method that returns a dict with the info organized by nutriment params"""
    if 'quantityContained' in det.keys() and type(det['quantityContained']) is list:
        l_det = []
        for d in det['quantityContained']:
            l_det.append({
                        'attribute':d['measurementUnitCode'],
                        'class': det['nutrientTypeCode']['value'],
                        'precision': det['measurementPrecision']['value'],
                        'value': d['value']
                        })
            prod_attrs_classes.add(det['nutrientTypeCode']['value'])
        return l_det
    prod_attrs_classes.add(det['nutrientTypeCode']['value'])
    return [{
            'attribute':det['quantityContained']['measurementUnitCode'] if 'quantityContained' in det.keys() else None,
            'class': det['nutrientTypeCode']['value'],
            'precision': det['measurementPrecision']['value'],
            'value': det['quantityContained']['value'] if 'quantityContained' in det.keys() else None
            }]

def obtain_nutriment_info(nh):
    """Method to organize all nutrimental info"""
    # First header
    if 'numberOfServings' in nh.keys():
        nhead = {'serving_type': 'numberOfServings',
                'serving_value': nh['numberOfServings']['value'],                
                'nutriments':[]}
        if 'nutrientBasisQuantity' in nh.keys():
            if type(nh['nutrientBasisQuantity']) is list:
                nhead['serving_unit']= nh['nutrientBasisQuantity'][0]['value']\
                                        +' '+nh['nutrientBasisQuantity'][0]['measurementUnitCode']
            else:
                nhead['serving_unit']= nh['nutrientBasisQuantity']['value']\
                                        +' '+nh['nutrientBasisQuantity']['measurementUnitCode']
        elif 'servingSize' in nh.keys():
            if type(nh['servingSize']) is list:
                nhead['serving_unit'] = nh['servingSize'][0]['value'] +' '+nh['servingSize'][0]['measurementUnitCode']
            else:
                nhead['serving_unit'] = nh['servingSize']['value'] +' '+nh['servingSize']['measurementUnitCode']
        else:
            nhead['serving_unit'] = None
    # Second Headers
    elif 'servingSize' in nh.keys():
        nhead = {'serving_type': 'servingSize',
                'nutriments':[]}
        if type(nh['servingSize']) is list:
            nhead.update({
                        'serving_value': nh['servingSize'][0]['value'],
                        'serving_unit': nh['servingSize'][0]['measurementUnitCode'],
            })
        else:
            nhead.update({
                        'serving_value': nh['servingSize']['value'],
                        'serving_unit': nh['servingSize']['measurementUnitCode'],
            })
    else :
        nhead = {'serving_type': 'notEspecified',
                'serving_value': None,
                'serving_unit': None,
                'nutriments':[]}
    if type(nh['nutrientDetail']) is list:
        for det in nh['nutrientDetail']:
            nhead['nutriments'] += nutrient_cast(det)
    else:
        nhead['nutriments'] += nutrient_cast(nh['nutrientDetail'])
    return nhead

def fetch_prod_instructions(mods, k, t):
    """Method to return Instructions after designated key and type"""
    l_inst = []
    for inst in mods['pim:productInstructionsModule'][k]:
        if type(inst) is not dict:
            return [{
                    'instructions': mods['pim:productInstructionsModule']\
                                            [k]['value'],
                    'instructions_type': t
                    }]
        if inst['languageCode'] == 'es':
            l_inst.append({
                            'instructions': inst['value'],
                            'instructions_type': t
                            })
    return l_inst

def allerge_info(al_dict):
    """Method to return all allergies info"""
    allerge = []
    if 'allergen' in al_dict.keys():
        for al in al_dict['allergen']:
            if type(al) is dict:
                allerge.append(dict(
                                content=al['levelOfContainmentCode']['value'] \
                                        if 'levelOfContainmentCode' in al.keys() else None,
                                allergy_type= al['allergenTypeCode']['value'] \
                                        if 'allergenTypeCode' in al.keys() else None,
                                instructions_type= 'allergen_code'
                ))
            else:
                allerge.append(dict(
                                content=al_dict['allergen']['levelOfContainmentCode']['value'] \
                                        if 'levelOfContainmentCode' in al_dict['allergen'].keys() else None,
                                allergy_type= al_dict['allergen']['allergenTypeCode']['value'] \
                                        if 'allergenTypeCode' in al_dict['allergen'].keys() else None,
                                instructions_type= 'allergen_code'
                ))
                break
    if 'allergenStatement' in al_dict.keys():
        for al in al_dict['allergenStatement']:
            if type(al) is dict:
                if al['languageCode'] == 'es':
                    allerge.append(dict(
                                    instructions= al['value'],
                                    instructions_type= 'allergic_statement'
                    ))
            else:
                allerge.append(dict(
                                    instructions= al_dict['allergenStatement']['value'],
                                    instructions_type= 'allergic_statement'
                    ))
                break
    return allerge

def obtain_gs1_api_data(prods_json):
    # Data munging
    for i, v in enumerate(prods_json):
        try:
            base_info = prods_json[i]['productDataRecord']['module'][0]['bpi:basicProductInformationModule']
            gtin = prods_json[i]['gtin']
            brand = base_info['brandNameInformation']['brandName']['value']
            #### Generar Func name como tag
            func_name = base_info['functionalName']['value'] if 'functionalName' in base_info.keys() else None
            category_code = base_info['gpcCategoryCode']['value'] if 'gpcCategoryCode' in base_info.keys() else None
            name = list_dict_parser_selected(base_info, 'productName', 'languageCode', 'es')
            #for mods in prods_json[i]['productDataRecord']['module']:
            #    attrs.add([y for y in mods.keys()][0])
            if 'imageLink' in base_info.keys():
                images = [x['url']['value'] for x in base_info['imageLink']] \
                if type(base_info['imageLink']) is list \
                else [base_info['imageLink']['url']['value']]
            else:
                images =[]
            provider = list_dict_parser(base_info, 'packagingSignatureLine', 'partyContactName', position=0)
            # Data validation module
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'product_tracking_information_module' in mods.keys():
                    val_dates, last_modified = [], None
                    for pti in mods['product_tracking_information_module']['history']:
                        if type(pti) is dict:
                            if pti['status']['value'] == 'VALIDATED':
                                val_dates.append(pti['createdDate']['value'] if 'createdDate' in pti.keys() \
                                                else pti['updatedDate']['value'])
                        else:
                            pti = mods['product_tracking_information_module']['history']
                            if pti['status']['value'] == 'VALIDATED':
                                val_dates.append(pti['createdDate']['value'] if 'createdDate' in pti.keys() \
                                                else pti['updatedDate']['value'])
                            break
                    if len(val_dates) > 0:
                        last_modified = max([datetime.datetime.strptime(ddf, "%Y-%m-%dT%H:%M:%S.%fZ") for ddf in val_dates])
            # Quantity attributes
            prod_attrs = []
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'pqi:productQuantityInformationModule' in mods.keys():
                    for pqi in mods['pqi:productQuantityInformationModule']:
                        if pqi == 'servingQuantityInformation':
                            prod_attrs_classes.add(pqi)
                            if mods['pqi:productQuantityInformationModule'][pqi]:
                                prod_attrs.append({'class': pqi,
                                                  'attribute': 'numberOfServingsPerPackage',
                                                  'value': mods['pqi:productQuantityInformationModule'][pqi]\
                                                           ['numberOfServingsPerPackage']['value'] \
                                                           if 'numberOfServingsPerPackage' in mods['pqi:productQuantityInformationModule'][pqi].keys()\
                                                            else mods['pqi:productQuantityInformationModule'][pqi]['measurementPrecisionCode']['value']})
                                prod_attrs_params.add('numberOfServingsPerPackage')
                        if pqi == 'drainedWeight' or pqi == 'netContent': #
                            prod_attrs_classes.add(pqi)
                            if mods['pqi:productQuantityInformationModule'][pqi]:
                                if type(mods['pqi:productQuantityInformationModule'][pqi]) is not list:
                                    prod_attrs.append({'class':pqi,
                                                        'attribute': mods['pqi:productQuantityInformationModule'][pqi]\
                                                               ['measurementUnitCode'],
                                                        'value': mods['pqi:productQuantityInformationModule'][pqi]\
                                                               ['value']})
                                    prod_attrs_params.add(mods['pqi:productQuantityInformationModule'][pqi]['measurementUnitCode'])
                                else:
                                    prod_attrs.append({'class':pqi,
                                                        'attribute': mods['pqi:productQuantityInformationModule'][pqi][0]\
                                                               ['measurementUnitCode'],
                                                        'value': mods['pqi:productQuantityInformationModule'][pqi][0]\
                                                               ['value']})
                                    prod_attrs_params.add(mods['pqi:productQuantityInformationModule'][pqi][0]['measurementUnitCode'])
            # Nutritional info
            nutr_attrs = []
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'npi:nutritionalProductInformationModule' in mods.keys():
                    if type(mods['npi:nutritionalProductInformationModule']['nutrientHeader']) is not list:
                        nutr_attrs.append(obtain_nutriment_info(mods['npi:nutritionalProductInformationModule']\
                                                                    ['nutrientHeader']))
                    else:
                        for nh in mods['npi:nutritionalProductInformationModule']['nutrientHeader']:
                            nutr_attrs.append(obtain_nutriment_info(nh))
            # Instructions
            additional = []
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'pim:productInstructionsModule' in mods.keys():
                    if 'consumerStorageInstructions' in mods['pim:productInstructionsModule']:
                        additional += fetch_prod_instructions(mods, 'consumerStorageInstructions', 'storage_instructions')
                    if 'consumerUsageInstructions' in mods['pim:productInstructionsModule']:
                        additional += fetch_prod_instructions(mods, 'consumerUsageInstructions', 'usage_instructions')
            # Food and Beverage Ingredients
            ingredients, ingreds = [], ''
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'fabii:foodAndBeverageIngredientInformationModule' in mods.keys():
                    # Not going to use parsed ingredients
                    # if 'foodAndBeverageIngredient' in mods['fabii:foodAndBeverageIngredientInformationModule']:
                    #    print(type(mods['fabii:foodAndBeverageIngredientInformationModule']['foodAndBeverageIngredient']))
                    #    for fabi in mods['fabii:foodAndBeverageIngredientInformationModule']['foodAndBeverageIngredient']:
                    #       print(fabi)
                    if 'ingredientStatement' in mods['fabii:foodAndBeverageIngredientInformationModule']:
                        for fabi in mods['fabii:foodAndBeverageIngredientInformationModule']['ingredientStatement']:
                            if type(fabi) is not dict:
                                ingreds = mods['fabii:foodAndBeverageIngredientInformationModule']\
                                                ['ingredientStatement']['value']
                                break
                            if fabi['languageCode'] == 'es':
                                ingreds = fabi['value']

                    if 'additivesStatement' in mods['fabii:foodAndBeverageIngredientInformationModule']:
                        for fabi in mods['fabii:foodAndBeverageIngredientInformationModule']['additivesStatement']:
                            if type(fabi) is not dict:
                                ingreds = mods['fabii:foodAndBeverageIngredientInformationModule']\
                                                ['additivesStatement']['value']
                                break
                            if fabi['languageCode'] == 'es':
                                ingredients.append(fabi['value'])   
                    

            # Claims
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'pcae:productClaimsAndEndorsementsModule' in mods.keys():
                    if 'nutritionalClaimStatement' in mods['pcae:productClaimsAndEndorsementsModule']:
                        if type(mods['pcae:productClaimsAndEndorsementsModule']['nutritionalClaimStatement']) is dict:
                            additional.append({
                                                'instructions': mods['pcae:productClaimsAndEndorsementsModule']\
                                                                    ['nutritionalClaimStatement']['value'],
                                                'instructions_type':'nutritional_statement'
                            })
                        elif type(mods['pcae:productClaimsAndEndorsementsModule']['nutritionalClaimStatement']) is list:
                            for ncs in mods['pcae:productClaimsAndEndorsementsModule']['nutritionalClaimStatement']:
                                if ncs['languageCode'] == 'es':
                                    additional.append({
                                                'instructions': ncs['value'],
                                                'instructions_type':'nutritional_statement'
                                                })
                    if 'warningStatement' in mods['pcae:productClaimsAndEndorsementsModule']:
                        if type(mods['pcae:productClaimsAndEndorsementsModule']['warningStatement']) is dict:
                            additional.append({
                                                'instructions': mods['pcae:productClaimsAndEndorsementsModule']\
                                                                ['warningStatement']['value'],
                                                'intructions_type': 'warning_statement'
                                                })
                        elif type(mods['pcae:productClaimsAndEndorsementsModule']['warningStatement']) is list:
                            for ncs in  mods['pcae:productClaimsAndEndorsementsModule']['warningStatement']:
                                if ncs['languageCode'] == 'es':
                                    additional.append({
                                                'instructions': ncs['value'],
                                                'instructions_type': 'warning_statement'
                                                })
                    if 'nutritionalClaimCode' in mods['pcae:productClaimsAndEndorsementsModule']:
                        for ncc in mods['pcae:productClaimsAndEndorsementsModule']['nutritionalClaimCode']:
                            if type(ncc) is dict:
                                additional.append({
                                                'instructions': ncc['value'],
                                                'instructions_type': 'nutritional_claim_code'
                                                })
                            else:
                                additional.append({
                                                'instructions': mods['pcae:productClaimsAndEndorsementsModule']\
                                                                    ['nutritionalClaimCode']['value'],
                                                'instructions_type': 'nutritional_claim_code'
                                                })
                                break
                    if 'dietaryClaimCode' in mods['pcae:productClaimsAndEndorsementsModule']:
                        for ncc in mods['pcae:productClaimsAndEndorsementsModule']['dietaryClaimCode']:
                            if type(ncc) is dict:
                                additional.append({
                                                'instructions': ncc['value'],
                                                'instructions_type': 'dietary_claim_code'
                                                })
                            else:
                                additional.append({
                                                'instructions': mods['pcae:productClaimsAndEndorsementsModule']\
                                                                    ['dietaryClaimCode']['value'],
                                                'instructions_type': 'dietary_claim_code'
                                                })
                                break
            # None food info
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'nfii:nonfoodIngredientInformationModule' in mods.keys():
                    if 'nonfoodIngredientStatement' in  mods['nfii:nonfoodIngredientInformationModule'].keys():
                        for nfis in mods['nfii:nonfoodIngredientInformationModule']['nonfoodIngredientStatement']:
                            if type(nfis) is dict:
                                if nfis['languageCode'] == 'es':
                                    additional.append({
                                                    'instructions': nfis['value'],
                                                    'instructions_type': 'non_food_information'
                                                    })
                            else:
                                additional.append({
                                                'instructions': mods['nfii:nonfoodIngredientInformationModule']\
                                                                    ['nonfoodIngredientStatement']['value'],
                                                'instructions_type': 'non_food_information'
                                                })
                                break
            # Allergies Info
            for mods in prods_json[i]['productDataRecord']['module']:
                if 'pai:productAllergenInformationModule' in mods.keys():
                    if 'allergenRelatedInformation' in mods['pai:productAllergenInformationModule'].keys():
                        for ari in mods['pai:productAllergenInformationModule']['allergenRelatedInformation']:
                            if type(ari) is dict:
                                additional += allerge_info(ari)
                            else:
                                additional += allerge_info( mods['pai:productAllergenInformationModule']\
                                                                   ['allergenRelatedInformation'])
                                break
            # INFO DEPLOYMENT
            print('**************************')
            save_all_item({
                            'retailer':'gs1',
                            'gtin' :gtin,
                            'brand': brand,
                            'category_code': category_code,
                            'name' : name,
                            'provider' : provider,
                            'images' : images,
                            'nutr_attrs' : nutr_attrs,
                            'ingreds' : ingreds,
                            'prod_attrs' : prod_attrs,
                            'additional' : additional
                            })
            
        except Exception as e:
            print(e)
            break

if __name__ == '__main__':
    verify_retailer()
    # Categorize call function
    categorize(categ_json, None, force=True)
    print('Saved Categories')
    # Attributes Class call function
    attributize(force=False)
    # Call API 

    headers = {
            'Accept-version': '1.2.0',
            'Authorization': 'Bearer 99855dd0-1fdc-11e7-938f-57b12629ece3'
        }

    products_list_endpoint = "https://mexico.q-aggregator.com/productlist/?f=j&pageNumber="
    product_detail_endpoint = "https://api.mexico.q-aggregator.com/product?gtin="
    for k in range(1, 2600, 100):
        for i in range(k, k+100):
            print('###################################')
            print(products_list_endpoint+str(i))
            print('###################################')
            r = requests.get(products_list_endpoint+str(i), headers=headers)
            if r.status_code == 200 and r.json()['status']:
                obtain_gs1_api_data(r.json()['data'])
        break
