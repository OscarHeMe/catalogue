#!/bin/bash

echo "Generating NginX config file..."
env/bin/python -m app.scripts.nginx_conf "$REGION" "$ROUTE"