FROM public.ecr.aws/lambda/python:3.7
USER root
RUN yum install -y krb5-devel gcc
RUN pip3 install --no-cache-dir openpyxl geopandas pandas numpy networkx gspread oauth2client apiclient python-lambda google-api-python-client arcgis python-dateutil requests
COPY src/ .
COPY .env/ ../.env/
COPY data/ ../data/
CMD [ "service.handler" ]