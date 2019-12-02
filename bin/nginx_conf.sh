#!/bin/bash

echo "Generating NginX config file..."
pipenv run python -m app.scripts.nginx_conf "$REGION" "$ROUTE"