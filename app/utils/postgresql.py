import os
import logging
import psycopg2

logger = logging.getLogger(__name__)


class Postgresql:

    def __init__(self):
        logger.debug('Initiating Postgressql Connection Class ')
        self.connection_parameter = None
        self._connection = None
        self._cursor = None

    def set_connection_parameter(self):
        self.connection_parameter = {
            "user": os.environ.get('SQL_USER'),
            "password": os.environ.get('SQL_PASSWORD'),
            "host": os.environ.get('SQL_HOST'),
            "port": os.environ.get('SQL_PORT'),
            "database": os.environ.get('SQL_DB')
        }

        return self.connection_parameter

    def create_connection(self):
        if self.connection_parameter is None:
            self.set_connection_parameter()

        try:
            logger.debug('Creating connection')
            conn = psycopg2.connect(user=self.connection_parameter['user'],
                                    password=self.connection_parameter['password'],
                                    host=self.connection_parameter['host'],
                                    port=self.connection_parameter['port'],
                                    database=self.connection_parameter['database'])

            self._connection = conn
            logger.debug("Connection parameter {}".format(self.connection_parameter))
        except Exception as ce:
            logger.error('Error in making connection with host={}, port={}, user={}, database={}'.format(
                self.connection_parameter['host'], self.connection_parameter['port'], self.connection_parameter['user'],
                self.connection_parameter['database']), exc_info=True)
            raise Exception("Connection Error with Postgressql")

    def get_cursor(self):
        self._cursor = self.connection.cursor()
        return self._cursor

    def close_connection(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as e:
                logger.warn('Unable to close connection. May be the connection is already closed', exc_info=True)
            self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self.create_connection()
        return self._connection

    @property
    def cursor(self):
        if self._cursor is None:
            return self.get_cursor()
        elif self._cursor.closed:
            del self._cursor
            return self.get_cursor()
        return self._cursor