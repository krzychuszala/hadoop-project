import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max
from pyspark import SparkContext
from pyspark.sql.functions import when, lit
import threading
import random
import time
import concurrent.futures

def check_nodes(spark):
    status = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
    active_nodes = len(status)
    print(f"Number of active nodes: {active_nodes}")
    if active_nodes >= 3:
        print("There are at least 3 nodes running.")
    else:
        print("Less than 3 nodes are running.")

def main():
    # spark = SparkSession.builder.appName("Product Rating Analysis").getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("Product Rating Analysis") \
    #     .master("spark://hadoop-master:7077") \
    #     .getOrCreate()
    #     # .config("spark.executor.instances", "3") \
    #     # .config("spark.executor.cores", "2") \
    #     # .config("spark.executor.memory", "2g") \
            
    spark = SparkSession.builder \
        .appName("Product Rating Analysis") \
        .master("yarn") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-master:9000") \
        .config("spark.yarn.jars", "hdfs://hadoop-master:9000/spark-jars/*.jar") \
        .getOrCreate()
        
    sc = spark.sparkContext("My Hadoop Cluster")
    master_url = sc.master
    print(f"Master URL: {master_url}")
    
    if master_url.startswith("local"):
        print("Running in local mode")
    elif master_url.startswith("spark"):
        print("Running in standalone mode")
    elif master_url.startswith("yarn"):
        print("Running in yarn mode")
    else:
        print("Running in unknown mode")
        
    df = spark.read.csv('../data/data.csv', header=True, inferSchema=True)
    df = df.drop('Timestamp')

    while True:
        df.show(3)
        print("Select an option:")
        print("1. Categorize Data, Grouped by ProductId")
        print("2. Find Min and Max Scores")
        print("3. Add New Data")
        print("4. Update Existing Data")
        print("5. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            categorize_data(df).show()
        elif choice == '2':
            min_rating, max_rating = find_min_max(df)
            print(f"Lowest Rating: {min_rating}, Highest Rating: {max_rating}")
        elif choice == '3':
            new_data = input("Enter new data (UserId,ProductId,Rating) separated by commas: ")
            new_data = [tuple(new_data.split(','))]
            columns = ["UserId", "ProductId", "Rating"]
            df = add_data(df, new_data, columns)
            df.filter(col("UserId") == new_data[0][0] & col("ProductId") == new_data[0][1]).show()
        elif choice == '4':
            new_data = input("Update row with (UserId,ProductId) with (Rating), please write these 3 values separated by commas: ")
            new_data = new_data.replace(" ", "")
            new_data = new_data.split(',')
            user_id, product_id, new_rating = new_data
            df = update_data(df, user_id, product_id, new_rating)
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")

    spark.stop()



def categorize_data(df):
    return df.groupBy("ProductId").avg("Rating").withColumnRenamed("avg(Rating)", "AverageRating")

def find_min_max(df):
    min_rating = df.select(spark_min("Rating")).collect()[0][0]
    max_rating = df.select(spark_max("Rating")).collect()[0][0]
    return min_rating, max_rating

def add_data(df, new_data, columns, spark=SparkSession.builder.appName("Product Rating Analysis").getOrCreate()):
    new_df = spark.createDataFrame(new_data, schema=columns)
    return df.union(new_df)

def update_data(df, user_id, product_id, new_rating):
    condition = (col("UserId") == user_id) & (col("ProductId") == product_id)
    if not df[condition].isEmpty():
        df = df.withColumn(
            'Rating',
            when(condition, new_rating).otherwise(col("Rating"))
        )
        print(f"Updated UserId: {user_id}, ProductId: {product_id} with new Rating: {new_rating}")
        df.filter(condition).show()
        return df
    else:
        print(f"No entry found for UserId: {user_id}, ProductId: {product_id}")

    return df

def run_stress_tests(df):
    def task_1():
        for _ in range(10000):
            categorize_data(df)

    def task_2():
        requests = [random.randint(1, 100) for _ in range(10000)]
        for req in requests:
            find_min_max(df)

    def task_3():
        for i in range(1000):
            add_data(df, [(i, f"Product_{i}", random.randint(1, 5))], ["UserId", "ProductId", "Rating"])

    print("Running Stress Test 1: Single client making the same request very quickly")
    start_time = time.time()
    task_1()
    print(f"Stress Test 1 completed in {time.time() - start_time:.2f} seconds")

    print("Running Stress Test 2: Multiple clients making requests randomly")
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(task_2) for _ in range(10)]
        concurrent.futures.wait(futures)
    print(f"Stress Test 2 completed in {time.time() - start_time:.2f} seconds")

    print("Running Stress Test 3: System processing a large load of data quickly")
    start_time = time.time()
    task_3()
    print(f"Stress Test 3 completed in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
