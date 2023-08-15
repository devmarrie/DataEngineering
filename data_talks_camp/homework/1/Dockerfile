FROM python:3.10-alpine3.18

RUN apt-get install wget
RUN pip install pandas, psycopg2-binary, sqlalchemy

WORKDIR /app
COPY ingest.py ingest.py

ENTRYPOINT [ "python", "ingest.py" ]