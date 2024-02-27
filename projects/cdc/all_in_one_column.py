import psycopg2

def info_change(conn):
    """who and when"""
    cursor = conn.cursor()

    cursor.execute(
        """
        ALTER TABLE transactions ADD COLUMN change_info JSONB;
        """
    )

    cursor.execute(
        """
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
        """
    )

    cursor.execute(
        """
        CREATE TRIGGER trigger_record_info_update
        BEFORE UPDATE ON transactions
        FOR EACH ROW EXECUTE FUNCTION record_changed_columns();
        """
    )
    
    cursor.close()
    conn.commit()

if __name__ == '__main__':
    conn = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5432
    )
    info_change(conn)