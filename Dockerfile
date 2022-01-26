FROM public.ecr.aws/lambda/python:latest
USER root
RUN pip install --no-cache-dir geopandas pandas numpy networkx gspread oauth2client apiclient python-lambda google-api-python-client arcgis pip install python-dateutil requests
COPY src/ .
COPY .env/ ../.env/
COPY data/ ../data/
CMD [ "service.handler" ]