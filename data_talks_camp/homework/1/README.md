1. --iidfile string
2. 3
3. `SELECT COUNT(1) FROM green_taxi_data WHERE DATE_TRUNC('day', lpep_pickup_datetime) = '2019-01-15' AND DATE_TRUNC('day', lpep_dropoff_datetime) = '2019-01-15';` - 20530
4. `SELECT DATE(lpep_pickup_datetime),MAX(trip_distance) AS mx FROM green_taxi_data GROUP BY DATE(lpep_pickup_datetime ORDER BY MAX(trip_distance) DESC;` - 2019-01-15

