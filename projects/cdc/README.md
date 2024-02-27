## Change Data Capture Streaming
Capture changes reflected in our database in real-time.

**Replicate the before and after value in columns**

ALTER TABLE transactions REPLICA IDENTITY FULL;

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

**Add the modified by and modified at columns**

`ALTER TABLE transactions ADD COLUMN modified_by TEXT;`

`ALTER TABLE transactions ADD COLUMN modified_at TIMESTAMP;`

**Create the function to update modified by and at**

```
CREATE OR REPLACE FUNCTION record_change_user()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_by := current_user;
    NEW.modified_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

```

**Create a trigger for the above function**

```
CREATE TRIGGER trigger_record_user_update
BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION record_change_user();
```

**Now put everything in one column called info**

`ALTER TABLE transactions ADD COLUMN change_info JSONB;`

**capture the changes to specific columns into json object**

```
CREATE OR REPLACE FUNCTION record_changed_columns()
RETURNS TRIGGER AS $$
DECLARE
    change_details JSONB;
BEGIN
    change_details := '{}'::JSONB; -- Initialize an empty JSONB object

    -- Check each column for changes and record as necessary
    IF NEW.amount IS DISTINCT FROM OLD.amount THEN
        change_details := jsonb_insert(change_details, '{amount}', jsonb_build_object('old', OLD.amount, 'new', NEW.amount));
    END IF;

    -- Add user and timestamp
    change_details := change_details || jsonb_build_object('modified_by', current_user, 'modified_at', now());

    -- Update the change_info column
    NEW.change_info := change_details;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**create a trigger for the function**

```
CREATE TRIGGER trigger_record_info_update
BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION record_changed_columns();
```
