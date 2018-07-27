import json
from config import *
import app.utils.db as db
import os
from pprint import pprint
import datetime
import ast
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import JSON

# Vademecum info file
vad_file = 'data/vademecum/vademecum_byprice_info.csv'

# DB Connector and models
def connect_sqlalch():
    return create_engine("postgresql://{}:{}@{}:{}/{}"
                         .format(SQL_USER,
                                 SQL_PASSWORD,
                                 SQL_HOST,
                                 SQL_PORT,
                                 SQL_DB))

print('Connecting to Catalogue PSQL....')
_conn = connect_sqlalch()


if __name__ == '__main__':
    # Read File
    df = pd.read_csv(vad_file)
    # Clean NaNs
    df.fillna('', inplace=True)
    # Set index
    df.set_index('item_uuid', inplace=True)
    d_info = []
    # Format elements
    for iu,v in df.to_dict(orient='index').items():
        d_info.append({
            'item_uuid': iu,
            'data': json.dumps({k: {"content": c} for k,c in v.items()})
        })
    # Generate DF
    vadem = pd.DataFrame(d_info)
    vadem.set_index(['item_uuid'], inplace=True)
    vadem['data'] = vadem['data'].astype(str)
    # Set Blacklists -> consult list of blacklisted
    vadem['blacklisted'] = False
    # Load into DB
    print('Loading into DB..')
    try:
        vadem.to_sql('item_vademecum_info',
                    _conn,
                    if_exists='replace',
                    dtype={'item_uuid': UUID, 'data': JSON},
                    chunksize=2000)
        print('Correctly stored Vademecum info in DB')
    except Exception as e:
        print('Error loading in DB:', e)