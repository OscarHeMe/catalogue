# -*- coding: utf-8 -*-
from pygres import Pygres
import json
import os

def connect_psql_identity():
    return Pygres(dict(
        SQL_HOST = "identity.byprice.db",
        SQL_DB = "identity",
        SQL_USER = "byprice",
        SQL_PASSWORD = os.getenv("IDENTITY_PASSWORD", "postgres"),
        SQL_PORT="5434"
    ))

def connect_psql_items():
   return Pygres(dict(
        SQL_HOST = "items.byprice.db",
        SQL_DB = "items",
        SQL_USER = "byprice",
        SQL_PASSWORD = os.getenv("ITEMS_PASSWORD", "postgres"),
        SQL_PORT="5433"
    ))
