#!/bin/bash

cd /Users/gamba/Nginx/byprice/byprice-item
. env/bin/activate


export LOG_LEVEL=DEBUG
gunicorn --workers 3 --bind unix:byprice-item.sock -m 000 wsgi:app

