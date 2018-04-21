# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession, DataFrameReader
from pyspark.sql.types import  *
from pyspark.sql import functions as F
import datetime
from uuid import uuid1
import requests 
import sys
import os
from pprint import pprint as pp
from pygres import Pygres
import pandas as pd

# Spark ExtraClass vars
SPARK_POSTGRESQL_JAR = os.getenv("SPARK_POSTGRESQL_JAR", "/srv/spark/jars/postgresql-42.1.1.jar")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
# Migration Sources
SQL_IDENTITY = os.getenv("SQL_IDENTITY_HOST", "192.168.99.100")
SQL_ITEMS = os.getenv("SQL_ITEMS", "192.168.99.100")
M_SQL_USER = os.getenv("M_SQL_USER", "postgres")
M_SQL_PASSWORD = os.getenv("M_SQL_PASSWORD", "ByPrice123!")
# Migration destination
SQL_HOST = os.getenv('SQL_HOST', "192.168.99.100")
SQL_DB = os.getenv('SQL_DB','catalogue')
SQL_DB = SQL_DB if os.getenv("ENV", "LOCAL") != 'DEV' else SQL_DB+'_dev'
SQL_USER = os.getenv('SQL_USER','postgres')
SQL_PASSWORD = os.getenv('SQL_PASSWORD','ByPrice123!')
SQL_PORT = os.getenv('SQL_PORT','5432')

def create_sc():
    """ Spark Context
    """
    conf = SparkConf()\
        .setAppName('Items_Identity_to_Catalogue')\
        .set("spark.executor.memory","3g")\
        .set("spark.executor.extraClassPath", SPARK_POSTGRESQL_JAR)\
        .set('spark.driver.extraClassPath', SPARK_POSTGRESQL_JAR)\
        .set('spark.jars', 'file:'+SPARK_POSTGRESQL_JAR)\
        .setMaster(SPARK_MASTER)
    sc = SparkContext(conf=conf)
    return sc

def create_sqlctx(sc):
    """ SQL Spark Context
    """
    return SQLContext(sc)

def create_session():
    """ Spark Session
    """
    return SparkSession\
                .builder\
                .getOrCreate()

def connect_psql():
    return Pygres(dict(
        SQL_HOST = SQL_HOST,
        SQL_DB = SQL_DB,
        SQL_USER = SQL_USER,
        SQL_PASSWORD = SQL_PASSWORD,
        SQL_PORT=SQL_PORT
    ))

class SQLTable(object):
    """ Pyspark SQL table
    """

    @staticmethod
    def read(spark, host, port, db, table):
        """ Creates JDBC Spark SQL connector
        """
        return spark.read\
            .jdbc("jdbc:postgresql://{}:{}/{}"\
                    .format(host, port, db),
                table=table,
                properties={
                        "user": M_SQL_USER,
                        "password": M_SQL_PASSWORD
                })


class Identity(object):
    """ Identity PSQL database
    """

    __tables__ = ['gtin', 'gtin_retailer', 
        'gtin_retailer_attribute']

    def __init__(self, spark, host, port):
        """ Constructor thats all SQL-spark connected tables
        """
        self.db = 'identity'
        for _t in self.__tables__:
            print('Initiating Identity.{} .'.format(_t))
            self.__dict__[_t] = SQLTable.read(spark,
                                    host, port, self.db, _t)


class Items(object):
    """ Items PSQL database
    """

    __tables__ = ['item', 'retailer', 'attribute',
        'attribute_class', 'attribute', 'brand', 
        'category', 'ingredient', 'provider',
        'item_provider', 'item_attribute',
        'item_brand', 'item_category', 'item_ingredient',
        'item_retailer', 'item']

    def __init__(self, spark, host, port):
        """ Constructor thats all SQL-spark connected tables
        """
        self.db = 'items'
        for _t in self.__tables__:
            print('Initiating Items.{} .'.format(_t))
            self.__dict__[_t] = SQLTable.read(spark,
                                    host, port, self.db, _t)


class Catalogue(object):
    """ Catalogue PSQL database
    """

    __tables__ = ['source', 'clss', 'attr', 'category',
        'item', 'product', 'product_image', 'product_attr',
        'product_category']

    @staticmethod    
    def write_source(identity, items):
        """ Migrate to source

            Schema:
            ```
                key : str,
                name : str,
                logo : str,
                type : str,
                retailer int,
                hierarchy int
            ```
        """
        _not_retailers = ['ims', 'mara', 'nielsen', 'plm', 'gs1']
        print('Populating retailer `source`\'s...')
        _source = items.retailer\
                    .where(~items.retailer.key.isin(_not_retailers))\
                    .withColumn('retailer', F.lit(1))\
                    .withColumn('type', F.lit('retailer'))\
                    .write\
                    .jdbc('jdbc:postgresql://{}:{}/{}'\
                            .format(SQL_HOST, SQL_PORT, SQL_DB),
                            table='source',
                            mode='append',
                            properties={
                                'user': SQL_USER,
                                'password': SQL_PASSWORD
                            })
        print('Populated retailer sources, now start with the rest..')
        _source = items.retailer\
                    .where(items.retailer.key.isin(_not_retailers))\
                    .withColumn('retailer', F.lit(1))\
                    .withColumn('type', F.lit('retailer'))\
                    .write\
                    .jdbc('jdbc:postgresql://{}:{}/{}'\
                            .format(SQL_HOST, SQL_PORT, SQL_DB),
                            table='source',
                            mode='append',
                            properties={
                                'user': SQL_USER,
                                'password': SQL_PASSWORD
                            })
        print('Finished populating `source` table!')

    @staticmethod    
    def write_clss(identity, items):
        """ Migrate to clss

            Schema:
            ```
                id_clss int,
                name str,
                name_es str,
                match str,
                key str,
                description str,
                source str
            ```
        """
        print('Populating `clss`\'s...')
        _clss = items.attribute_class\
                .withColumnRenamed('id_attribute_class', 'id_clss')\
                .write\
                .jdbc('jdbc:postgresql://{}:{}/{}'\
                        .format(SQL_HOST, SQL_PORT, SQL_DB),
                    table='clss',
                    mode='append',
                    properties={
                        'user': SQL_USER,
                        'password': SQL_PASSWORD
                    })
        print('Finished populating `clss` table!')
    
    @staticmethod    
    def write_attr(identity, items):
        """ Migrate to attr

            Schema:
            ```
                id_attr int,
                id_clss int,
                name str,
                key str,
                match str,
                has_value int,
                meta json,
                source str
            ```
        """
        print('Populating `attr`\'s...')
        items.attribute\
                .withColumnRenamed('id_attribute_class', 'id_clss')\
                .withColumnRenamed('id_attribute', 'id_attr')\
                .write\
                .jdbc('jdbc:postgresql://{}:{}/{}'\
                        .format(SQL_HOST, SQL_PORT, SQL_DB),
                    table='attr',
                    mode='append',
                    properties={
                        'user': SQL_USER,
                        'password': SQL_PASSWORD
                    })
        print('Finished populating `attr` table!')
    
    @staticmethod    
    def write_category(identity, items, psql):
        """ Migrate to category

            Schema:
            ```
                id_category int,
                id_parent int,
                source str,
                name str,
                key str,
                code str
            ```
        """
        print('Populating `category`\'s...')
        items.category\
                .withColumnRenamed('retailer', 'source')\
                .drop(items.category.retailer_reference)\
                .drop(items.category.id_parent)\
                .write\
                .jdbc('jdbc:postgresql://{}:{}/{}'\
                        .format(SQL_HOST, SQL_PORT, SQL_DB),
                    table='category',
                    mode='append',
                    properties={
                        'user': SQL_USER,
                        'password': SQL_PASSWORD
                    })
        print('Batch created categories, now updating parents...')
        cat_model = psql.model("category", "id_category")
        cats = items.category\
            .select('id_category', 'id_parent')\
            .toPandas()
        cats.fillna('', inplace=True)
        # Updates all records to add id_parent's
        for i, row in cats.iterrows():
            if not row.id_parent:
                continue
            try:
                cat_model.id_category = int(row.id_category)
                cat_model.id_parent = int(row.id_parent)
                cat_model.save()
            except Exception as e:
                print(e)
        print('Finished updating `category` table!')
    
    @staticmethod    
    def write_item(identity, items):
        """ Migrate to category

            Schema:
            ```
                item_uuid : str (uuid)
                gtin : str (len(14))
                checksum : int,
                name : str
                description str,
                last_modified datetime
            ```
        """
        print('Populating `item`\'s...')
        # Extract and format tables
        gtin14 = F.udf(lambda x: str(x).zfill(14)[-14:])
        _gtin = identity.gtin\
                .select('item_uuid',
                        gtin14(identity.gtin.gtin).alias('gtin'),
                        'checksum',
                        'name')
        _item = items.item\
                .select('item_uuid',
                        'description',
                        'last_modified')
        # Join Identity.gtin and Items.item tables
        check_mod = F.udf(lambda x: x if x \
                        else datetime.datetime.utcnow(), TimestampType())
        cat_item = _gtin\
                    .join(_item,
                        on='item_uuid',
                        how='left_outer')
        df_item = cat_item\
                    .withColumn('last_modified',
                            check_mod(cat_item.last_modified)\
                            .alias('last_modified'))\
                    .toPandas()
        print('Collected data, now writing in DB..')
        from sqlalchemy import create_engine
        _conn = create_engine("postgresql://{}:{}@{}:{}/{}"\
            .format(SQL_USER, SQL_PASSWORD, SQL_HOST, SQL_PORT, SQL_DB))
        df_item\
            .set_index('item_uuid')\
            .to_sql('item', _conn, if_exists='append')
        print('Finished writing to `item` table')   
    

if __name__ == '__main__':
    # Call to create Spark context
    print('Generating PySpark Context ...')
    sc = create_sc()
    sqlctx = create_sqlctx(sc)
    spark = create_session()
    print('PySpark Context running, now running JDBC connector...')
    # Load all tables from both DBs
    items = Items(spark, SQL_ITEMS, 5432)
    identity = Identity(spark, SQL_IDENTITY, 5432)
    # Addition Pygres connector
    psql = connect_psql()
    print('Initialized Item and Identity tables!')
    # Populate Sources
    #Catalogue.write_source(identity, items)
    # Populate Clss
    #Catalogue.write_clss(identity, items)
    # Populate Attr
    #Catalogue.write_attr(identity, items)
    # Populate Category
    #Catalogue.write_category(identity, items, psql)
    # Populate Item
    #Catalogue.write_item(identity, items)

    # Close connector
    psql.close()
    print('Finished migration execution!')