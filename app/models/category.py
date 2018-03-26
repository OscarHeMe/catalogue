import datetime
from flask import g
from app import errors, logger
from config import *
import requests
from pprint import pprint, pformat as pf
import ast
import json
from app.norm.normalize_text import key_format
from collections import defaultdict

# Multilevel dictionary
dd=lambda:defaultdict(dd)


class Category(object):
    """
        Class perform Query methods on PostgreSQL items
    """

    @staticmethod
    def get_all(retailer):
        """
            Get categories from retailer
        """
        q = g._db.query("SELECT * FROM category where retailer = %s ", (retailer,)).fetch()
        return q


    @staticmethod
    def get_ims(ctype="atc",levels=[3,4], nested=True):
        """ Get ims categories given the root parent and the level
        """
        # Get id_category of the ctype
        parents = g._db.query("select id_category, id_parent, code, name, key from category where id_parent = (select id_category from category where key = %s);", (ctype,)).fetch()
        # Loop to get all categories
        if not parents:
            return []

        def get_children(parents, children=[]):
            for p in parents:
                rows = g._db.query("""
                    select id_category, id_parent, name, key, code from category 
                    where id_parent = %s
                """, (p['id_category'],)).fetch()
                if not rows:
                    return parents
                parents = parents+get_children(rows, children)
            return parents

        # Loop all children
        categories = parents + get_children(parents)

        def level_length(ctype, lvl):
            lvl = int(lvl)
            if ctype == 'atc':
                if lvl == 1: return 1
                elif lvl == 2: return 3
                elif lvl == 3: return 4
                elif lvl == 4: return 5
            if ctype == 'ch':
                if lvl == 1: return 2
                elif lvl == 2: return 3
                elif lvl == 3: return 4
                elif lvl == 4: return 5

        # Level
        result = []
        levels = [ level_length(ctype,l) for l in levels ]
        for cat in categories:
            # Level of category
            if len(cat['code']) in levels:
                result.append(cat)

        if not nested:
            return sorted(result, key=lambda k: k['code'])

        def branch(category):
            # Get the immediate children
            children = [ cat for cat in result if cat['id_parent'] == category['id_category'] ]
            if len(children) > 0:
                category['nested'] = children
                for i,ch in enumerate(children):
                    children[i]['nested'] = branch(ch)
                return children
            else:
                return []

        id_parents = [ cat['id_parent'] for cat in result]
        id_categories = [ cat['id_category'] for cat in result]
        id_grandparents = set(id_parents) - set(id_categories)

        top_parents = [ cat for cat in result if cat['id_parent'] in id_grandparents ]
        tree=top_parents
        for i,cat in enumerate(tree):
            tree[i]['nested'] = branch(cat)

        return tree or []

