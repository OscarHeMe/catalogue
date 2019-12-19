FROM byprice/base-data-services:v2_3.6.8

# Copy service content
COPY ./ /catalogue/
RUN mkdir /logs

# Change workdir
WORKDIR /catalogue

# Install project dependencies
RUN pipenv install

VOLUME /var/log/catalogue

# App , environment & Logging
ENV APP_NAME='catalogue-production'
ENV APP_DIR='/'
# Bug with rabbit_engine file , not adding _dev for LOCAL
ENV ENV='PROD'
ENV FLASK_APP=app/__init__.py
ENV REGION='MEX'
ENV LOG_LEVEL='PROD'

# Streamer
ENV STREAMER='rabbitmq'
ENV STREAMER_HOST='rmq-analytics.byprice.com'
ENV STREAMER_PORT=5222
ENV STREAMER_QUEUE='catalogue'
ENV STREAMER_ROUTING_KEY='catalogue'
ENV STREAMER_EXCHANGE='data'
ENV STREAMER_EXCHANGE_TYPE='direct'
ENV STREAMER_VIRTUAL_HOST='mx'
ENV STREAMER_USER='mx_pubsub'

# Queues
ENV QUEUE_CACHE='cache'
ENV QUEUE_ROUTING='routing'
ENV QUEUE_CATALOGUE='catalogue'
ENV QUEUE_CATALOGUE_ITEM='catalogue_item'
ENV QUEUE_GEOPRICE='geoprice'
ENV QUEUE_GEOLOCATION='geolocation'

ENTRYPOINT /bin/bash /catalogue/bin/run.sh

