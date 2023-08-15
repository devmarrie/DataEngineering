FROM prefecthq/prefect:latest-python3.10

RUN  pip install --upgrade pip

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt

COPY cd /opt/prefect/flows
COPY data/yellow /opt/prefect/data/yellow