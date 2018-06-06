FROM python:2.7.15-stretch

MAINTAINER Amy Skerry <amy.skerry@gmail.com>


# Install everything else
ADD requirements.txt /tmp/requirements.txt
RUN python2.7 -m pip install -r /tmp/requirements.txt

# Set up shared folder
RUN mkdir /opt/shared_nbs
ADD notebooks /opt/shared_nbs
RUN chmod -R 777 /opt/shared_nbs

