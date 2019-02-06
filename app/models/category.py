import datetime
from flask import g
from app import errors
from ByHelpers import applogger
from config import *
import requests
from pprint import pprint, pformat as pf
import ast
import json
from app.norm.normalize_text import key_format

# Logger
logger = applogger.get_logger()

class Category(object):
    """ Class perform Query methods on PSQL Catalogue.category
    """

    __attrs__ = ['id_category', 'id_parent', 'source',
        'name', 'key', 'code']

    def __init__(self, _args):
        """ Category constructor

            Params:
            -----
            _args : dict
                All arguments to build a `Category` record
        """ 
        # Arguments verification and addition
        for _k in self.__attrs__:
            if _k in _args:
                self.__dict__[_k] = _args[_k]
                continue
            self.__dict__[_k] = None
        # Formatting needed params
        self.key = key_format(self.name)
    
    def save(self, commit=True):
        """ Class method to save Category in DB
        """
        logger.info("Saving category...")
        if self.id_category:
            if not Category.exists({'id_category': self.id_category}):
                raise errors.ApiError(70006, "Cannot update, Category not in DB!")
        elif Category.exists({'key': self.key, 'source': self.source}):
            self.message = 'Category already exists!'
            self.id_category = Category.get_id(self.name,
                                        self.source)
            return self.id_category
        # Load model
        m_cat = g._db.model("category", "id_category")
        for _at in self.__attrs__:
            if self.__dict__[_at]:
                m_cat.__dict__[_at] = self.__dict__[_at]
        try:
            # Save record
            self.message = "Category {} correctly!".format(\
                'updated' if self.id_category else 'stored')
            m_cat.save(commit=commit)
            self.id_category = m_cat.last_id
            logger.info(self.message \
                    + '({})'.format(self.id_category))
            return self.id_category
        except Exception as e:
            logger.error(e)
            return None
        
    @staticmethod
    def exists(k_param):
        """ Static method to verify Category existance

            Params:
            -----
            k_param : dict
                Key-value element to query in Category table

            Returns:
            -----
            exists : bool
                Existance flag
        """
        logger.debug("Verifying Category existance...")
        _where = ' AND '.join(["{}='{}'".format(*z) \
                            for z in list(k_param.items())])
        try:
            exists = g._db.query("""SELECT EXISTS (
                            SELECT 1 FROM category WHERE {} LIMIT 1)"""\
                            .format(_where))\
                        .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists
    
    @staticmethod
    def get_id(_cat, _source, _key='id_category'):
        """ Fetch ID from category

            Params:
            -----
            _cat : str
                Category name
            _source : str
                Source key
            _key : str ('id_category' | 'id_parent')
                Key-Column to retrieve value from

            Returns:
            -----
            _id : int
                Category or Parent ID
        """        
        try:
            _res = g._db\
                    .query("""SELECT {} 
                        FROM category
                        WHERE key = '{}'
                        AND source = '{}'
                        LIMIT 1"""\
                        .format(_key,
                            key_format(_cat),
                            _source))\
                    .fetch()
            if _res:
                return _res[0][_key]
        except Exception as e:
            logger.error(e)
        return None
    

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

    