import psycopg2

def replicate_change(conn):
    """ make the before and after change apear"""
    cursor = conn.cursor()

    cursor.execute(
        """
        ALTER TABLE transactions REPLICA IDENTITY FULL;
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
    replicate_change(conn)