#!/bin/bash

# Init the database
pipenv run flask initdb

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    pipenv run gunicorn --workers 3 --bind unix:$APP_NAME.sock -m 000 -t 200 wsgi:app &
    nginx -g "daemon off;"
elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER mode"
    pipenv run flask consumerb
fi

if [[ $MODE == "SERVICE_LOCAL" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    pipenv run gunicorn -b localhost:9000 wsgi:app --timeout=3600
fi
