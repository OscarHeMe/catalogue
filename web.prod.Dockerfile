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
ENV APP_NAME='catalogue-web-service-production'
ENV APP_DIR='/'
# Bug with rabbit_engine file , not adding _dev for LOCAL
ENV ENV='PROD'
ENV FLASK_APP=app/__init__.py
ENV REGION='MEX'
ENV LOG_LEVEL='INFO'

# Streamer
ENV STREAMER='rabbitmq'
ENV STREAMER_HOST='rmq-prod.byprice.com'
ENV STREAMER_PORT=5222
ENV STREAMER_QUEUE='catalogue'
ENV STREAMER_ROUTING_KEY='catalogue'
ENV STREAMER_EXCHANGE='data'
ENV STREAMER_EXCHANGE_TYPE='direct'
ENV STREAMER_VIRTUAL_HOST='mx'
ENV STREAMER_USER='mx_pubsub'
# ENV STREAMER_PASS from secret34.83.250.126

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

# Map ports
EXPOSE 8003

# Add Nginx configuration file
RUN bash bin/nginx_conf.sh
ADD cfn/nginx/conf.d/ /etc/nginx/conf.d
RUN rm -rf /etc/nginx/sites-available/default && rm -rf /etc/nginx/sites-enabled/default

ENTRYPOINT /bin/bash /catalogue/bin/run_web_service.sh
