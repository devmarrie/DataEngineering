## Top trending youtube videos
using the top trending videos to predict  how we can apply ads in one of the most popularly used platforms(youtube)
[dataset_link](https://www.kaggle.com/datasets/datasnaek/youtube-new)

copy the json reference files from data to s3
`aws s3 cp . s3://youtube-raw-carlifonia-dev/data/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"`

copy the csv files to their respective folder


```
aws s3 cp CAvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=ca/
aws s3 cp DEvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=de/
aws s3 cp FRvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=fr/
aws s3 cp GBvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=gb/
aws s3 cp INvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=in/
aws s3 cp JPvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=jp/
aws s3 cp KRvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=kr/
aws s3 cp MXvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=mx/
aws s3 cp RUvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=ru/
aws s3 cp USvideos.csv s3://youtube-raw-carlifonia-dev/data/raw_statistics_csv/region=us/
```