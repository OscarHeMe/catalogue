FROM byprice/base-data-services:v2_3.6.8

# Copy service content
COPY ./ /catalogue_item/
RUN mkdir /logs

# Change workdir
WORKDIR /catalogue_item

# Install project dependencies
RUN pipenv install

VOLUME /var/log/catalogue_item

# App , environment & Logging
ENV APP_NAME='catalogue_item-production-gcp'
ENV APP_DIR='/'
# Bug with rabbit_engine file , not adding _dev for LOCAL
ENV ENV='PROD'
ENV FLASK_APP=app/__init__.py
ENV REGION='MEX'
ENV LOG_LEVEL='DEBUG'

# Streamer
ENV STREAMER='rabbitmq'
ENV STREAMER_HOST='rmq-prod.byprice.com'
ENV STREAMER_PORT=5222
ENV STREAMER_QUEUE='catalogue_item'
ENV STREAMER_ROUTING_KEY='catalogue_item'
ENV STREAMER_EXCHANGE='data'
ENV STREAMER_EXCHANGE_TYPE='direct'
ENV STREAMER_VIRTUAL_HOST='mx'
ENV STREAMER_USER='mx_pubsub'
# ENV STREAMER_PASS from secret

# Queues
ENV QUEUE_CACHE='cache'
ENV QUEUE_ROUTING='routing'
ENV QUEUE_CATALOGUE='catalogue'
ENV QUEUE_CATALOGUE_ITEM='catalogue_item'
ENV QUEUE_GEOPRICE='geoprice'
ENV QUEUE_GEOLOCATION='geolocation'

# Celert
ENV C_FORCE_ROOT='true'

# Postgres
ENV SQL_DB='catalogue'
ENV SQL_HOST='35.233.170.43'
ENV SQL_PASSWORD='byprice'
ENV SQL_PORT=5432
ENV SQL_USER='byprice'
#ENV SQL_PASSWORD from secret

ENTRYPOINT /bin/bash /catalogue_item/bin/run_item_data_service.sh
