import config
import unittest
import pandas as pd
from app.utils.postgresql import Postgresql


psql_db = Postgresql()


products = [
    {
        'name'  : 'Limpiador Multiusos Fabuloso Pasión de Frutas 1 L',
        'source': 'rappi',
        'gtin'  : '07509546008295',
        'product_id': '00000000975397655',
    },
    {
        'name'  : 'Limpiador Multiusos Fabuloso Pasión de Frutas 1 L',
        'source': 'chedraui',
        'gtin'  : '07509546008295',
        'product_id': '7539q55',
    },
    {
        'name'  : 'FABULOSO PASION DE FRUTAS 1 LT',
        'source': 'paris',
        'gtin'  : '07509546008295',
        'product_id': '7509546008295',
    },
    {
        'name'  : 'Limpiador multiusos',
        'source': 'fresko',
        'gtin'  : '',
        'product_id': '00000007509546008295',
    },
    {
        'name'  : 'Limpiador Multiusos FaPasión de frutas',
        'source': 'chedraui',
        'gtin'  : '0750954600829',
        'product_id': '00000000750954600829',
    },
    {
        'name'  : 'Frazada Cola de Sirena Mermaids 123  Arcoíris',
        'source': 'walmart_online',
        'gtin'  : '0750032690296',
        'product_id': '00000000750032690296',
    }
]


def update_prod_query(data_batch, table, pkey, cols=[]) -> list:
    values = []
    p_uuids = []
    sets = []
    if len(cols) > 0:
        for el in cols:
            sets.append('{} = %({})s'.format(el, el))
        qry = "UPDATE product SET {} WHERE {} = %({})s".format(','.join(sets), pkey, pkey)    
        for data in data_batch:
            n_data = {}
            pval = data.get(pkey)
            vs = []
            ks = []
            for k in cols:
                value = data.get(k, None)
                if isinstance(value, str) or isinstance(value, list):
                    value = "'" + str(value).replace('%', '%%').replace("'", "''") + "'"
                # elif not value and not isinstance(value, bool):
                #     continue

                vs.append(str(value))
                ks.append(str(k))
                n_data[k] = value

            if n_data:
                # print(tp)
                values.append(data)
                p_uuids.append(pval)

    if len(values) > 0:
        print(qry)
        print(values[:2])
        try:
            psql_db.cursor.executemany(qry, tuple(values))
        except Exception as e:
            logger.error('Error while trying to update {}:\n   - {}'.format([-1], e))
    psql_db.connection.commit()                   
    return p_uuids


def insert_batch_qry(data_batch, table, pkey, cols=[]):
    values = []
    qry = ''
    if len(cols) > 0:
        for data in data_batch:
            vs = []
            for k in cols:
                value = data.get(k, None)
                if isinstance(value, str):
                    value = "'" + value.replace('%', '%%') + "'"
                elif not value:
                    value = 'NULL'

                vs.append(str(value))

            if len(vs) > 0:
                values.append("(" + ",".join(vs) + ")")

    if len(values) > 0:
        qry = """INSERT INTO {} ({}) VALUES {} RETURNING {};""".format(table,
                                                                    ','.join(cols), 
                                                                    ','.join(values),
                                                                    pkey)
                                              
        print(qry)
        psql_db.cursor.execute(qry)
    return True


# class CatalogueQuerysTestCase(unittest.TestCase):
#     """ Test Case for Catalogue Connection
#     """

    # @classmethod
    # def setUpClass(cls):
    #     """ Initializes the database
    #     """
    #     # Define test database
    #     # if config.TESTING:
    #     #     with app.app.app_context():
    #     #         app.initdb()


    #@unittest.skip('Already tested')
    # def test_000_batch_exist(self):
if True:
    if True:
        """ Testing exist query in batch
        """ 
        print("Testing exist query in batch")


        to_update = []

        to_insert = []

        psql_db = Postgresql()

        # print(psql_db.connection)

        cur = psql_db.get_cursor()

        query = """SELECT product_uuid FROM product p WHERE p.product_id = %s AND p.source = %s;"""
        
        # vars_list = [(prod['product_id'], prod['source']) for prod in products]

        from pprint import pprint


        for prod in products:
            cur.execute(query, (prod['product_id'], prod['source']))
            result = cur.fetchall()
            if len(result) > 0:
                for tup in result:
                    p_uuid = tup[0]
                    if p_uuid:
                        data = prod.copy()
                        data.update({
                            'product_uuid' : p_uuid
                        })
                        to_update.append(data)
            else:
                data = prod
                to_insert.append(data)
            
        print('{} elements to insert'.format(len(to_insert)))

        cols = ['name', 'source', 'product_id']
        #pprint(to_insert)
        insert_batch_qry(to_insert, 'product', 'product_uuid', cols)


        #pprint(to_update)
        print('{} elements to update'.format(len(to_update)))
        update_prod_query(to_update, 'product', 'product_uuid', cols)

        # self.assertEqual(True, True)

psql_db.connection.commit()









        