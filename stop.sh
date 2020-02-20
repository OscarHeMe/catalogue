#!/bin/bash
export APP_DIR="/iqvia-colombia/web-services/catalogue"
cd ~$APP_DIR
source .envvars
# Stop Processes
kill -9 $(ps aux | grep gunicorn | grep $APP_NAME | grep $APP_DIR | awk '{print $2}')
# Clear Log Files
rm $PWD/logs/gunicorn*.log
