#!/bin/bash

# ENV
source .envvars
source env/bin/activate

# flask initdb
python -m scripts.populate

