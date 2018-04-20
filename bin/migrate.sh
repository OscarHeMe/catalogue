#!/bin/bash

# Set Test 
TEST_DB='F'

# Spark vars
SPARK_CLIENT="/srv/spark"
SPARK_MASTER="local[2]"
SPARK_JARS="$SPARK_CLIENT""/jars/py4j-0.10.1.jar"
JOB_PATH="scripts/migration.py"
export PYTHONPATH=$PYTHONPATH:$PWD
# Init DB
echo "Initializing DB..."
source env/bin/activate
source .envvars
flask initdb

echo "Starting PySpark migration job..."
$SPARK_CLIENT/bin/spark-submit --master $SPARK_MASTER --jars $SPARK_JARS $JOB_PATH
echo "Finished migration job!"

# Drop database 
if [ $TEST_DB = 'T' ]
then
    echo "Deleting Test DB"
    if [ $ENV = 'DEV' ]
    then
        SQL_DB=$SQL_DB"_dev"
    fi
    export PGPASSWORD=$SQL_PASSWORD
    psql -U $SQL_USER -h $SQL_HOST -w -c "DROP DATABASE $SQL_DB" 
fi
