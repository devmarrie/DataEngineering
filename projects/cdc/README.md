## Change Data Capture Streaming
Capture changes reflected in our database in real-time.

**Modify amounts in debezium and creating a connection directly using the terminal**
    curl -H 'Content-Type: application/json' localhost:8083/connectors --data '{
    "name": "postgres-connector",
    "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "topic.prefix": "cdc",
    "database.user": "postgres",
    "database.dbname": "financial_db",
    "database.hostname": "postgres",
    "database.password": "postgres",
    "plugin.name": "pgoutput",
    "decimal.handling.mode": "string"
    }
    }'