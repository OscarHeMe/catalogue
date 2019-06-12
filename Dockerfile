FROM byprice/base-python-web:latest

# Environment variables
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV FLASK_APP=app/__init__.py
ENV APP_NAME="catalogue"
ENV REGION="US"
ENV ROUTE="bpcatalogue"
ENV MODE="SERVICE"

# Copy repo
COPY ./ /catalogue/
RUN mkdir /catalogue/logs

# Change workdir
WORKDIR /catalogue

# Install local dependencies
RUN virtualenv env && env/bin/pip install -r requirements.txt

# Map ports
EXPOSE 8000
EXPOSE 80

VOLUME /var/log/catalogue

# Add Nginx configuration file
RUN bash bin/nginx_conf.sh
RUN cp cfn/nginx/conf.d/default.conf /etc/nginx/conf.d/
RUN rm -rf /etc/nginx/sites-available/default && rm -rf /etc/nginx/sites-enabled/default

ENTRYPOINT /bin/bash /catalogue/bin/run.sh
