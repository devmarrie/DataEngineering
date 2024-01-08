## Realtime Tesla Stock Analysis

### Overview

Tesla is ranked as the highest stock in the Nasdaq stock market, as shown in the image below.This project aims to utilize Tesla stock prices ranging from 04-01-2020 to 04-01-2024 to provide insights on trading strategies and the financial health of the stock.

![Stock ranking](imgs/stck_prediction.png)


### Questions the project aims to answer
- **Price trends:** Is the price trending upwards, downwards, and what is the current price of the stock?
- **Recent price movements**: What are the highest and lowest prices reached today?
- **Support and resistance levels:** Are there any significant price levels where the stock has repeatedly bounced off or failed to break through?
- **Financial heath:** Is the stock worth an investment today?
- **Trading volume:** How many shares have been traded today?
- **Unusual volume:** Is the trading volume significantly higher or lower than usual?
- **Correlation with price movements:** Is there a correlation between volume and price movements? (e.g., high volume on price increases, low volume on price decreases)

### Technologies used
- Python
- Pyspark
- Apache Kafka 
- Snowflake
- Docker(for deployment of pipelines)

### Dependancies
- Docker

### Running the application
- docker build -t kafka-spark-setup .
- docker run kafka-spark-setup (builds and runs the application)
- docker run -p 9092:9092 -p 2181:2181 kafka-spark-setup (This maps the container's ports 9092 (Kafka) and 2181 (ZooKeeper) to the same ports on your host machine for ease of access to external services)

*To run the docker compose*
- docker compose up -f docker_compose.yaml -up -d
- docker images
- docker ps
- docker excec -it kafka bin/sh 
- cd / then cd bin 
then move to the opt directory(move to that kafka version then bin to view the commands)


