FROM public.ecr.aws/lambda/python:3.8
USER root
RUN yum install -y krb5-devel gcc libffi-dev
RUN pip3 install --no-cache-dir geopandas pandas numpy networkx gspread oauth2client apiclient python-lambda google-api-python-client arcgis python-dateutil requests
RUN pip3 install -U requests[security]
COPY src/ .
COPY .env/ ../.env/
COPY data/ ../data/
CMD [ "service.handler" ]