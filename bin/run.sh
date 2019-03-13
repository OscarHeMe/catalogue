#!/bin/bash

/bin/bash ./env/bin/activate

# Init the database
./env/bin/flask initdb

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    ./env/bin/gunicorn --workers 3 --bind unix:$APP_NAME.sock -m 000 -t 200 wsgi:app &
    nginx -g "daemon off;"
elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER mode"
    ./env/bin/flask consumer
fi

if [[ $MODE == "SERVICE_LOCAL" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    ./env/bin/gunicorn -b localhost:9000 wsgi:app --timeout=3600
fi