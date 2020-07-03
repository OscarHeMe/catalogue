# coding: utf-8
from app import db
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Table, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.utils.postgresql import Postgresql
from app.utils.postgresql_queries import *
from psycopg2.extras import DictCursor

Base = declarative_base()
metadata = Base.metadata


class Cls(db.Model):
    __tablename__ = 'clss'
    __table_args__ = {'schema': 'public'}

    id_clss = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".clss_id_clss_seq'::regclass)"))
    name = Column(Text)
    name_es = Column(Text)
    match = Column(Text)
    key = Column(Text, index=True)
    description = Column(Text)
    source = Column(String(255))


class Item(db.Model):
    __tablename__ = 'item'
    __table_args__ = {'schema': 'public'}

    item_uuid = Column(UUID, primary_key=True, server_default=text("gen_random_uuid()"))
    gtin = Column(String(14))
    checksum = Column(Integer)
    name = Column(String(255))
    description = Column(Text)
    last_modified = Column(DateTime)
    page_views = Column(Integer, server_default=text("0"))


t_item_vademecum_info = Table(
    'item_vademecum_info', metadata,
    Column('item_uuid', UUID, index=True),
    Column('data', JSON),
    Column('blacklisted', Boolean),
    schema='public'
)


t_matching_unique = Table(
    'matching_unique', metadata,
    Column('source', String(255)),
    Column('product_id', String(255)),
    Column('product_uuid', UUID),
    Column('item_uuid', UUID),
    Column('gtin', Text),
    Column('name', Text),
    Column('description', Text),
    Column('brand', Text),
    schema='public'
)


t_mymatview = Table(
    'mymatview', metadata,
    Column('source', String(255)),
    Column('product_id', String(255)),
    schema='public'
)


t_product_normalized = Table(
    'product_normalized', metadata,
    Column('product_uuid', UUID, index=True),
    Column('normalized', Text),
    schema='public'
)


t_search_by_source = Table(
    'search_by_source', metadata,
    Column('name', Text),
    Column('source', Text),
    schema='public'
)


class Source(db.Model):
    __tablename__ = 'source'
    __table_args__ = {'schema': 'public'}

    key = Column(String(255), primary_key=True)
    name = Column(String(255))
    logo = Column(String(255))
    type = Column(String(255))
    retailer = Column(Integer)
    hierarchy = Column(Integer)


t_unique_by_source_product_id = Table(
    'unique_by_source_product_id', metadata,
    Column('source', String(255)),
    Column('product_id', String(255)),
    Column('product_uuid', UUID),
    Column('item_uuid', UUID),
    Column('name', Text),
    Column('description', Text),
    schema='public'
)


class Attr(db.Model):
    __tablename__ = 'attr'
    __table_args__ = {'schema': 'public'}

    id_attr = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".attr_id_attr_seq'::regclass)"))
    id_clss = Column(ForeignKey('public.clss.id_clss'), index=True)
    name = Column(Text)
    key = Column(Text, index=True)
    match = Column(Text)
    has_value = Column(Integer)
    meta = Column(JSON)
    source = Column(Text)

    cls = relationship('Cls')


class Category(db.Model):
    __tablename__ = 'category'
    __table_args__ = {'schema': 'public'}

    id_category = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".category_id_category_seq'::regclass)"))
    id_parent = Column(ForeignKey('public.category.id_category'))
    source = Column(ForeignKey('public.source.key'), index=True)
    name = Column(Text)
    key = Column(Text)
    code = Column(String(255))

    parent = relationship('Category', remote_side=[id_category])
    source1 = relationship('Source')


class Product(db.Model):
    __tablename__ = 'product'
    __table_args__ = {'schema': 'public'}

    external_allowed_columns = ['product_uuid', 'name', 'gtin', 'description', 
        'brand', 'categories', 'url', 'provider', 'ingredients'
    ]

    product_uuid = Column(UUID, primary_key=True, index=True, server_default=text("gen_random_uuid()"))
    item_uuid = Column(ForeignKey('public.item.item_uuid'), index=True)
    source = Column(String(255), nullable=False, index=True)
    product_id = Column(String(255), nullable=False, index=True)
    name = Column(String(255))
    gtin = Column(String(14))
    description = Column(Text)
    normalized = Column(Text)
    raw_product = Column(JSON)
    raw_html = Column(Text)
    categories = Column(Text)
    ingredients = Column(Text)
    brand = Column(Text)
    provider = Column(Text)
    url = Column(Text)
    images = Column(Text)
    last_modified = Column(DateTime)
    is_active = Column(Boolean)

    item = relationship('Item')
      
    @staticmethod
    def get_all_products(page, ipp):
        limit = ipp
        offset = int((page - 1)*limit)
        try:
            retailer_product = Product.query.limit(limit).offset(offset)
        except Exception as e:
            db.session.rollback()
            return False, e
        return Product.serialize_product(retailer_product)

    @staticmethod
    def paginate_all_products(page, ipp):
        try:
            retailer_product = Product.query.paginate(page, ipp, error_out=False)
        except Exception as e:
            db.session.rollback()
            return False, e
        return Product.serialize_product(retailer_product.items)

    @staticmethod
    def sql_paginate_products(page, ipp):
        limit = ipp
        offset = int((page - 1)*limit)
        try:
            psql_db = Postgresql()
            query = f"SELECT * FROM product" # LIMIT {limit} OFFSET {offset}"
            connection = psql_db.connection
            cursor = connection.cursor(name = 'server_cursor',
                                               cursor_factory = RealDictCursor,
                                               scrollable = True)
            cursor.execute(query, None)
            cursor.scroll(offset, mode = 'absolute')
        
            retailer_product = cursor.fetchmany(ipp)
            cursor.close()
            connection.close()
        except Exception as e:
            cursor.close()
            connection.close()
            return False, e
        return Product.serialize_product(retailer_product)


    @staticmethod
    def serialize_product(product):
        serialized_result = []
        all_column_names = [
            c.name for c in Product.__table__.columns]
        for row in product:
            temp_dict = {}
            if not isinstance(row, dict):
                dict_row = row.__dict__.copy()
            else:
                dict_row = row.copy()
            for column_name in all_column_names:
                temp_dict[column_name] = dict_row.get(column_name)
            serialized_result.append(temp_dict)

        return True, serialized_result


class ItemAttr(db.Model):
    __tablename__ = 'item_attr'
    __table_args__ = {'schema': 'public'}

    id_item_attr = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".item_attr_id_item_attr_seq'::regclass)"))
    id_attr = Column(ForeignKey('public.attr.id_attr'))
    item_uuid = Column(ForeignKey('public.item.item_uuid'))
    value = Column(Text)
    precision = Column(Text)
    last_modified = Column(DateTime)

    attr = relationship('Attr')
    item = relationship('Item')


class ProductAttr(db.Model):
    __tablename__ = 'product_attr'
    __table_args__ = {'schema': 'public'}

    id_product_attr = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".product_attr_id_product_attr_seq'::regclass)"))
    id_attr = Column(ForeignKey('public.attr.id_attr'))
    product_uuid = Column(ForeignKey('public.product.product_uuid'))
    value = Column(Text)
    precision = Column(Text)
    last_modified = Column(DateTime)

    attr = relationship('Attr')
    product = relationship('Product')


class ProductCategory(db.Model):
    __tablename__ = 'product_category'
    __table_args__ = {'schema': 'public'}

    id_product_category = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".product_category_id_product_category_seq'::regclass)"))
    id_category = Column(ForeignKey('public.category.id_category'))
    product_uuid = Column(ForeignKey('public.product.product_uuid'), index=True)
    last_modified = Column(DateTime)
    deprecated = Column(Integer)

    category = relationship('Category')
    product = relationship('Product')


class ProductImage(db.Model):
    __tablename__ = 'product_image'
    __table_args__ = {'schema': 'public'}

    id_product_image = Column(Integer, primary_key=True, server_default=text("nextval('\"public\".product_image_id_product_image_seq'::regclass)"))
    product_uuid = Column(ForeignKey('public.product.product_uuid'))
    image = Column(Text)
    descriptor = Column(JSON)
    last_modified = Column(DateTime)

    product = relationship('Product')
