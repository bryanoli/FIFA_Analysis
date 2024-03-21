FROM python:3.11.7

RUN pip install pandas sqlalchemy psycopg2
RUN apt-get install wget

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "bash" ]