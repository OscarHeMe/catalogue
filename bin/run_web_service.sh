#!/bin/bash
export MODE="SERVICE"
echo "[$(date)][CATALOGUE]: Activating virtual environment"


echo "[$(date)][CATALOGUE]: Loading env variables, if any"
source .envvars

# todo ?? Init the database
pipenv run flask initdb


is_running_gunicorn=$(ps aux | grep 'gunicorn' | wc -l)
if [ $is_running_gunicorn -gt 1 ]
    then
            echo "[$(date)][$APP_NAME]: Already running with GUNICORN in $MODE mode . Shutting down"
            ps aux | grep 'gunicorn'
            kill $(ps aux | grep 'gunicorn' | awk '{print $2}')
fi

is_running_flask_run=$(ps aux | grep 'flask consumer' | wc -l)
if [ $is_running_flask_run -gt 1 ]
    then
            echo "[$(date)][$APP_NAME]: Already running $APP_NAME with Flask Consumer in $MODE mode . Shutting down"
            ps aux | grep 'flask consumer'
            kill $(ps aux | grep 'flask consumer' | awk '{print $2}')
fi



echo "[$(date)][$APP_NAME]: Starting with GUNICORN in $MODE mode..."
pipenv run gunicorn --workers 3 --bind unix:catalogue.sock -m 000 -t 500 wsgi:app & nginx -g "daemon off;"
