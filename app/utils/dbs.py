# -*- coding: utf-8 -*-
from pygres import Pygres
import json
import os

def connect_psql_identity():
    return Pygres(dict(
        SQL_HOST = "localhost",
        SQL_DB = "identity",
        SQL_USER = "postgres",
        SQL_PASSWORD = os.getenv("IDENTITY_PASSWORD", "postgres"),
        SQL_PORT="5432"
    ))

def connect_psql_items():
   return Pygres(dict(
        SQL_HOST = "localhost",
        SQL_DB = "items",
        SQL_USER = "postgres",
        SQL_PASSWORD = os.getenv("ITEMS_PASSWORD", "postgres"),
        SQL_PORT="5432"
    ))
