FROM python:3.9.1
RUN pip install pandas psycopg2 sqlalchemy
WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY yellow_tripdata_2021-01.csv /app/
ENTRYPOINT [ "python", "ingest_data.py" ]

# sudo docker build -t taxi_ingest:v001 .
# run it reffer to book