The official airflow image curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

Incase you do not want to see alot of things in the docker-compose.yml

```
version: '3'
services:
    postgres:
        image: postgres:13
        env_file:
            - .env
        volumes:
            - postgres-db-volume:/var/lib/postgresql/data
        healthcheck:
            test: ["CMD", "pg_isready", "-U", "airflow"]
            interval: 5s
            retries: 5
        restart: always

    scheduler:
        build: .
        command: scheduler
        restart: on-failure
        depends_on:
            - postgres
        env_file:
            - .env
        volumes:
            - ./dags:/opt/airflow/dags
            - ./logs:/opt/airflow/logs
            - ./plugins:/opt/airflow/plugins
            - ./scripts:/opt/airflow/scripts
            - ~/.google/credentials/:/.google/credentials


    webserver:
        build: .
        entrypoint: ./scripts/entrypoint.sh
        restart: on-failure
        depends_on:
            - postgres
            - scheduler
        env_file:
            - .env
        volumes:
            - ./dags:/opt/airflow/dags
            - ./logs:/opt/airflow/logs
            - ./plugins:/opt/airflow/plugins
            - ~/.google/credentials/:/.google/credentials:ro
            - ./scripts:/opt/airflow/scripts

        user: "${AIRFLOW_UID:-50000}:0"
        ports:
            - "8080:8080"
        healthcheck:
            test: [ "CMD-SHELL", "[ -f /home/airflow/airflow-webserver.pid ]" ]
            interval: 30s
            timeout: 30s
            retries: 3

volumes:
  postgres-db-volume:
```

On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user. You have to make sure to configure them for the docker-compose:
dags - stores the airflow pipelines
logs - store communication between the scheduler and the workers
plugins - utilities you may need to use within your dags.

```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

