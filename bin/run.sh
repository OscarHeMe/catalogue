#!/bin/bash
export MODE="CONSUMER"
echo "[$(date)][CATALOGUE]: Activating virtual environment"


echo "[$(date)][CATALOGUE]: Loading env variables, if any"
source .envvars

# todo ?? Init the database
pipenv run flask initdb


is_running_gunicorn=$(ps aux | grep 'gunicorn' | wc -l)
if [ $is_running_gunicorn -gt 1 ]
    then
            echo "[$(date)][CATALOGUE]: Already running with GUNICORN. Shutting down"
            ps aux | grep 'gunicorn'
            kill $(ps aux | grep 'gunicorn' | awk '{print $2}')
fi

is_running_flask_run=$(ps aux | grep 'flask consumer' | wc -l)
if [ $is_running_flask_run -gt 1 ]
    then
            echo "[$(date)][CATALOGUE]: Already running with FLASK CONSUMER. Shutting down"
            ps aux | grep 'flask consumer'
            kill $(ps aux | grep 'flask consumer' | awk '{print $2}')
fi

pipenv run flask consumer

