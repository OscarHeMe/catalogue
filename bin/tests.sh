#!/bin/bash

cd $APP_DIR
. env/bin/activate
. .envvars

# Init the database
./env/bin/flask initdb

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run gunicorn tests
    echo "Starting $APP_NAME in SERVICE testing.."
    ./env/bin/python -m app.tests.tests_service
elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER testing..."
    ./env/bin/python -m app.tests.tests_consumer
fi

