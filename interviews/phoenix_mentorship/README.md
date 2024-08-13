## ETL pipleine for Agriculture data
![architecture_img](img/phoenix_img.png)
Extract from a remote postgresDB,
Ingest it  locally as shown [here](load_data.py).

[Transform](transform_data.py) the data by removing columns full of empty data, duplicates and changing the datatypes.


Load to Supabase - https://supabase.com/docs.
Create a database, a table and [load the data](load_data.py).