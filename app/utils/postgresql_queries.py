import logging
from psycopg2.extras import execute_values, RealDictCursor

logger = logging.getLogger()
BULK_INSERT_CHUNKSIZE = 1000


def execute_select(conn, query, values=None, get_dict=False):
    logger.debug(query)
    try:
        if get_dict:
            cursor = conn.cursor(cursor_factory = RealDictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute(query, values)
        return cursor
    except Exception as e:
        logger.error('Error in executing select query = {}'.format(query), exc_info=True)
        raise Exception('Executing Select Query Error. Connection={}'.format(conn))


# cursor closes automatically when goes out of scope
def execute_insert(conn, query, values=None):
    logger.debug(query)
    try:
        cursor = conn.cursor()
        if values is None:
            cursor.execute(query)
        else:
            cursor.execute(query, values)
        conn.commit()
        result = {
            'row_count': cursor.rowcount,
            'message': cursor.statusmessage
        }
        cursor.close()
        return result
    except Exception as e:
        logger.error('Error in executing insert query = {}'.format(query), exc_info=True)
        raise Exception('Executing Insert Query Error. Connection={}'.format(conn))

# http://initd.org/psycopg/docs/extras.html
# https://stackoverflow.com/questions/44131544/python-creating-string-from-dict-items-for-writing-to-postgresql-db
def execute_bulk_insert(conn, query, values, template, page_size=BULK_INSERT_CHUNKSIZE):
    logger.debug(query[:1000])
    try:
        cursor = conn.cursor()
        # todo check if can return number of rows affected or inserted and not the cursor

        execute_values(cursor, query, values, template=template, page_size=page_size)
        conn.commit()
        cursor.close()
        return
    except Exception as e:
        logger.error('Error in executing bulk insert query = {}'.format(query), exc_info=True)
        raise Exception('Executing Bulk Insert Query Error. Connection={}'.format(conn))


def execute_query(conn, query):
    logger.debug(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        return cursor
    except Exception as e:
        logger.error('Error in executing query = {}'.format(query), exc_info=True)
        raise Exception('Executing Query Error. Connection={}'.format(conn))