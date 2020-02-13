import os
import logging
import psycopg2

logger = logging.getLogger(__name__)


class Postgresql:

    def __init__(self):
        logger.debug('Initiating Postgressql Connection Class ')
        self._connection_parameter = None
        self._connection = None
        self._cursor = None

    def set_connection_parameter(self, **kwargs):
        self._connection_parameter = {
            "user": os.environ.get('SQL_USER') if not kwargs.get('user') else kwargs.get('user'),
            "password": os.environ.get('SQL_PASSWORD') if not kwargs.get('password') else kwargs.get('password'),
            "host": os.environ.get('SQL_HOST') if not kwargs.get('host') else kwargs.get('host'),
            "port": os.environ.get('SQL_PORT') if not kwargs.get('port') else kwargs.get('port'),
            "database": os.environ.get('SQL_DB') if not kwargs.get('database') else kwargs.get('database')
        }

    def get_connection_parameter(self):
        return self._connection_parameter

    @property
    def connection(self):
        if self._connection is None or self._connection.closed != 0:
            self.set_connection()
        return self._connection

    def set_connection(self):
        if self._connection_parameter is None:
            self.set_connection_parameter()

        try:
            logger.debug('Creating postgres connection')
            conn = psycopg2.connect(user=self._connection_parameter['user'],
                                    password=self._connection_parameter['password'],
                                    host=self._connection_parameter['host'],
                                    port=self._connection_parameter['port'],
                                    database=self._connection_parameter['database'])

            self._connection = conn
            logger.info('Connection Successful. Connection={}'.format(str(self.connection)))
        except Exception as ce:
            logger.error('Error in making postgres connection with host={}, port={}, user={}, database={}'.format(
                self._connection_parameter['host'], self._connection_parameter['port'], self._connection_parameter['user'],
                self._connection_parameter['database']), exc_info=True)
            raise Exception("Connection Error with Postgresql connection={}".format(str(self._connection)))
    
    def get_connection(self):
        # Read-only integer attribute: 0 if the connection is open, nonzero if it is closed or broken.
        if self._connection is None or self._connection.closed != 0:
            self.set_connection()
        return self._connection
        
    def close_connection(self):
        if self._connection is not None and self._connection.closed == 0:
            try:
                self._connection.commit()
                self._connection.close()
            except Exception as e:
                logger.warning('Unable to close postgres connection. Connection might be already closed', exc_info=True)
                logger.warning('connection={}'.format(str(self._connection)))
            finally:
                self._connection = None

    # use if not sure if connection exist or in try catch
    def get_cursor(self):
        return self.get_connection().cursor() 

