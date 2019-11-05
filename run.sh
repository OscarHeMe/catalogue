#!/bin/bash

deactivate

source ../.envvars-data
source ./.envvars

pipenv run flask initdb

# Run Service
if [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER mode"
    pipenv run flask consumer
# Evaluate the mode of execution and the 
elif [[ $MODE == "SERVICE" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    pipenv run gunicorn --workers 3 --bind unix:$APP_NAME.sock -m 000 -t 200 wsgi:app &
    nginx -g "daemon off;"
elif [[ $MODE == "SERVICE_LOCAL" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    pipenv run -b localhost:9000 wsgi:app --timeout=3600
fi
