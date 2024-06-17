import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max
from pyspark import SparkContext
from pyspark.sql.functions import when, lit
import threading
import random
import time
import concurrent.futures

def main():
    # spark = SparkSession.builder \
    #     .appName("Product Rating Analysis") \
    #     .master("yarn") \
    #     .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-master:9000") \
    #     .config("spark.executor.instances", "3") \
    #     .config("spark.executor.cores", "2") \
    #     .config("spark.executor.memory", "1g") \
    #     .getOrCreate()
            
    # spark = SparkSession.builder \
    #     .appName("Product Rating Analysis") \
    #     .master("yarn") \
    #     .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-master:9000") \
    #     .config("spark.yarn.jars", "hdfs://hadoop-master:9000/spark-jars/hadoop-mapreduce-examples-2.7.1-sources.jar") \
    #     .getOrCreate()
        
    spark = SparkSession.builder.appName("Final Project").getOrCreate()    
        
    df = spark.read.csv("hdfs://localhost:9000/user/input/data.csv", header=True, inferSchema=True)
    df = df.drop('Timestamp')

    while True:
        print("\n")
        df.show(3)
        print("Select an option:")
        print("1. Average Rating per Product")
        print("2. Average Rating per User")
        print("3. Add new data")
        print("4. Update data")
        print("5. Get dataset min rating")
        print("6. Get dataset max rating")
        print("7. Run Stress Test 1")
        print("8. Run Stress Test 2")
        print("9. Run Stress Test 3")
        print("10. Exit")
        
        choice = input("Enter your choice: \n")

        if choice == '1':
            get_avg_rating_per_product(df).show(5)
        elif choice == '2':
            get_avg_rating_per_user(df).show(5)
        elif choice == '3':
            print("Addition of new data")
            user_id = input("Enter UserId: ")
            product_id = input("Enter ProductId: ")
            rating = input("Enter Rating: ")
            rating = float(rating)
            df = add_data(df, [(user_id, product_id, rating)], spark)
            df.filter((col("UserId") == user_id) & (col("ProductId") == product_id)).show(5)
        elif choice == '4':
            print("Update data")
            user_id = input("Enter UserId: ")
            product_id = input("Enter ProductId: ")
            new_rating = input("Enter new Rating: ")
            new_rating = float(new_rating)
            df = update_data(df, user_id, product_id, new_rating)
            df.filter((col("UserId") == user_id) & (col("ProductId") == product_id)).show(5)
        elif choice == '5':
            print("Min Rating: "+str(get_min_rating(df)))
        elif choice == '6':
            print("Max Rating: "+str(get_max_rating(df)))
        elif choice == '7':
            print("Running Stress Test 1: Single client making the same request very quickly")
            start_time = time.time()
            task_1(df)
            print("Stress Test 1 completed in", time.time() - start_time, "seconds")
        elif choice == '8':
            print("Running Stress Test 2: Multiple clients making requests randomly")
            start_time = time.time()
            task_2_test(df)
            print("Stress Test 2 completed in", time.time() - start_time, "seconds")                
        elif choice == '9':
            print("Running Stress Test 3: System processing a large load of data quickly")
            start_time = time.time()
            task_3_add(df, spark)
            print("Stress Test 3 completed in", time.time() - start_time, "seconds")
        elif choice == '10':
            break
        else:
            print("Invalid choice. Please try again.")

    spark.stop()


def get_avg_rating_per_product(df):
    return df.groupBy("ProductId").avg("Rating").withColumnRenamed("avg(Rating)", "AverageRating")

def get_avg_rating_per_user(df):
    return df.groupBy("UserId").avg("Rating").withColumnRenamed("avg(Rating)", "AverageRating")

def get_min_rating(df):
    return df.select(spark_min("Rating")).first()[0]

def get_max_rating(df):
    return df.select(spark_max("Rating")).first()[0]

def add_data(df, new_data, spark):
    new_df = spark.createDataFrame(new_data, schema=df.schema)
    return df.union(new_df)

def update_data(df, user_id, product_id, new_rating):
    condition = (col("UserId") == user_id) & (col("ProductId") == product_id)
    if df.filter(condition).count() > 0:
        df = df.withColumn(
            'Rating',
            when(condition, new_rating).otherwise(col("Rating"))
        )
        return df
    else:
        pass
    return df

def task_1(df):
    for _ in range(10000):
        get_avg_rating_per_product(df)

def task_2(df, no_requests):
    requests = [random.randint(1, 100) for _ in range(no_requests)]
    for req in requests:
        if req % 2 == 0:
            get_max_rating(df)
        else:
            get_min_rating(df)
            
def task_2_test(df):
    client1 = random.randint(1, 10000)
    client2 = random.randint(1, 10000 - client1)
    client3 = 10000 - client1 - client2
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(task_2, df, client1), executor.submit(task_2, df, client2), executor.submit(task_2, df, client3)]
        try:
            concurrent.futures.wait(futures)
        except KeyboardInterrupt:
            executor.shutdown(wait=False) 
            print("Tasks interrupted and executor shut down.")

def task_3_add(df, spark):
    for i in range(1000):
        add_data(df, [(i, "product"+str(i), random.uniform(1, 5) )], spark)
        
if __name__ == "__main__":
    main()
