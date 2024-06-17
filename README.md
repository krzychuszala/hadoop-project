## Project Name: Spark-Based Interactive Product Rating Analyzer

### Description:

This project provides an interactive Spark-based tool for analyzing product ratings data. It offers features like calculating average ratings, adding/updating ratings, and conducting stress tests to evaluate system performance under load.

### Database Schema:

![image](https://github.com/krzychuszala/hadoop-project/assets/59472045/3021946c-8c93-4f3c-b405-ebf25ff34ace)

### Prerequisites:

- Apache Spark (installed and configured)
- Hadoop Distributed File System (HDFS) for storing data
- A CSV file named data.csv containing ratings data (header: UserID, ProductID, Rating)
- PySpark library (installed)

### How to run it?

1. Creating docker container
git clone https://github.com/AdamGodzinski/Pyspark-BDaDP.git
docker build . -t hadoopspark
docker run --name hadoop -p 8088:8088 -p 9870:9870 -p
9864:9864 -p 10000:10000 -p 8032:8032 -p 8030:8030 -p
8031:8031 -p 9000:9000 -p 8888:8888 -itd hadoopspark
docker exec -it -u 0 hadoop /bin/bash

2. Running envinronment (inside container)
service ssh restart
sudo su hadoop
start-all.sh

3. Running console app
- go to directory  /home/hadoop/scripts
- run command -> python3 main.py
