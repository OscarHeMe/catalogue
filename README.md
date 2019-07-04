# Byprice Catalogue Service (MX Version)

Service to query all item details, like images, ingredients, attributes and additional info.

## Concepts & Entities

* Source: Data sources, it can be a retailer, data provider, laboratory, etc...
* Item: Group of items gathered mostly by GTIN coincidence (previously item)
* Product: Item representation on a retailer's catalogue, as attribute they have: product_id, source, item_uuid (previously item_retailer)
* Clss: Class of attributes
* Attr: Attribute, gathers anything related to a specific **item** (id, retailer)

## How to setup?

1. Install and configure nginx, setup a server that listens to a unix socket named `./catalogue.sock`
2. Check that you have an open connection to the db
3. Install virtualenv `pip install virtualenv`
4. Setup the virtual environment `virtualenv env`
5. Activate the virtuale environment `source env/bin/activate`
6. Install python dependencies `pip install -r requirements.txt`

## Configuration

Set the execution mode of the service through the **MODE** environment variable

```shell
export MODE='<SERVICE|CONSUMER>'
```

App env vars

```shell
export FLASK_APP='app/__init__.py'
export APP_DIR='<home dir of the app: $PWD>'
export APP_NAME='<app name>'
export ENV='<DEV|PROD>'
```

PostgreSQL DB env vars

```shell
export SQL_HOST='<postgresql ip or hostname>'
export SQL_PORT='<postgresql port number>'
export SQL_USER='<postgresql username>'
export SQL_PASSWORD='<postgresql passwd>'
export SQL_DB='<postgresql db name: items>'
export SRV_GEOLOCATION='<url to the geolocation service>'
```

Consumer env vars

```shell
export STREAMER='rabbitmq'
export STREAMER_HOST='<ip or hostname>'
export STREAMER_ROUTING_KEY=''
export STREAMER_EXCHANGE='data'
export STREAMER_EXCHANGE_TYPE='direct'
export STREAMER_USER="<user>"
export STREAMER_PASS="<pass>"
export STREAMER_VIRTUAL_HOST="<vhost>"
```

Logger env vars

```shell
export LOG_LEVEL='<DEBUG|INFO|...>'
export LOG_HOST='<remote logging host>'
export LOG_PORT='<remote logging port>'
```

## How do I run it?

### Web Service

```bash
. envvars && MODE='SERVICE' $APP_DIR/bin/run.sh
```

#### Manually

1. Activate virtual environments `source env/bin/activate`
2. Export environment variables `source .envvars`
3. Initialize database `flask initdb`
4. Run gunicorn process `gunicorn --workers 3 --bind unix:byprice-item.sock -m 000 wsgi:app`

### Consumer

```bash
. envvars && MODE='CONSUMER' $APP_DIR/bin/run.sh
```

### Consumer for item

- Build docker image with `./Dockerfile.item` template:

```bash
docker build --no-cache -t <amazon prefix>/catalogue_item:dev -f Dockerfile.item .
```

#### Manually

1. Activate virtual environments `source env/bin/activate`
2. Export environment variables `source .envvars`
3. Initialize database `flask initdb`
4. Run gunicorn process `flask consumer`

## Testing

Set the environment variable `$MODE` to the test the consumer or the web service and then run the following:

```bash
source bin/tests.sh
```


## License

ByPrice Copyright (c) 2018

## To Do

- Populate Ingredients from Past DB of ByPrice
- Populate Brands from Past DB of ByPrice
