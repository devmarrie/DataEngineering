import psycopg2
import pandas as pd

host = 'aws-0-sa-east-1.pooler.supabase.com'
database_name = 'postgres'
port_id = 6543
user = 'postgres.vtthhncofpkrylaqpabj'
password = 'juM70PkREv4HDxVm'
table_name = 'raw_agric_data'

try:
    connection = psycopg2.connect(
        host=host,
        database=database_name,
        user=user,
        password=password,
        port=port_id
    )
    cursor = connection.cursor()
    query = f'SELECT * FROM {table_name};'
    cursor.execute(query)

    data = cursor.fetchall()

    # check the output
    # count = 0
    # for row in data:
    #     if count <= 10:
    #         print(row)
    #         count += 1

    df = pd.DataFrame(data, columns=[d[0] for d in cursor.description])
    print(df.head())
except Exception as e:
    print(f'Error:{e}')
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()