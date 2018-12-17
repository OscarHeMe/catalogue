FROM byprice-base-serv

MAINTAINER ByPrice


# Environment variables
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV FLASK_APP=app/__init__.py
ENV APP_NAME="catalogue"
ENV REGION="US"
ENV ROUTE="bpcatalogue"
ENV MODE="SERVICE"

RUN apt-get update
RUN apt-get install -y python3-tk --force-yes
RUN apt-get install -y libglib2.0-0 --force-yes
RUN apt-get install -y libsm6 libxext6 --force-yes

# Copy Repo content
COPY ./ /catalogue/
RUN mkdir /catalogue/logs

# Change workdir
WORKDIR /catalogue

# Populate the database
RUN virtualenv env && env/bin/pip install -r requirements.txt

# Se mapean los puertos con docker run -P
EXPOSE 8000
EXPOSE 80

VOLUME /var/log/catalogue

# Add Nginx configuration file
RUN bash bin/nginx_conf.sh
RUN cp cfn/nginx/conf.d/default.conf /etc/nginx/conf.d/
RUN rm -rf /etc/nginx/sites-available/default && rm -rf /etc/nginx/sites-enabled/default

ENTRYPOINT /bin/bash /catalogue/bin/run.sh