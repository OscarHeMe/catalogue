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

    def set_connection_parameter(self, **kwargs):
        self.connection_parameter = {
            "user": os.environ.get('SQL_USER') if not kwargs.get('user') else kwargs.get('user'),
            "password": os.environ.get('SQL_PASSWORD') if not kwargs.get('password') else kwargs.get('password'),
            "host": os.environ.get('SQL_HOST') if not kwargs.get('host') else kwargs.get('host'),
            "port": os.environ.get('SQL_PORT') if not kwargs.get('port') else kwargs.get('port'),
            "database": os.environ.get('SQL_DB') if not kwargs.get('database') else kwargs.get('database')
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

    def commit(self):
        self.connection.commit()
    
    def rollback(self):
        self.connection.rollback()
        
    def disconnect(self, commit=True):
        if commit:
            self.commit()
        else:
            self.rollback()
        self.connection.close()        

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