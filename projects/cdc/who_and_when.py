import psycopg2

def record_change(conn):
    """who and when"""
    cursor = conn.cursor()

    cursor.execute(
        """
        ALTER TABLE transactions ADD COLUMN modified_by TEXT;
        ALTER TABLE transactions ADD COLUMN modified_at TIMESTAMP;
        """
    )

    cursor.execute(
        """
        CREATE OR REPLACE FUNCTION record_change_user()
        RETURNS TRIGGER AS $$
        BEGIN
        NEW.modified_by := current_user;
        NEW.modified_at := CURRENT_TIMESTAMP;
        RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    cursor.execute(
        """
        CREATE TRIGGER trigger_record_user_update
        BEFORE UPDATE ON transactions
        FOR EACH ROW EXECUTE FUNCTION record_change_user();
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
    record_change(conn)