from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy

def create_spark_session():
    try:
        spark= SparkSession.builder\
        .appName("spark-streaming")\
        .config("spark.cassandra.connection.host", "cassandra")\
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra")\
        .config("spark.cassandra.auth.password", "cassandra")\
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

def connect_to_cassandra():
    auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
    try:
        cluster= Cluster(['cassandra'], 
                         port=9042, 
                         auth_provider=auth,
                         load_balancing_policy=RoundRobinPolicy(),
                         protocol_version=5)
        cassandra= cluster.connect()
        logging.info("Connected to Cassandra successfully.")
        return cassandra
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None

def create_keyspace(cassandra):
    try:
        cassandra.execute("""
                          CREATE KEYSPACE IF NOT EXISTS worldwide_users
                          WITH REPLICATION = {
                          'class': 'SimpleStrategy',
                          'replication_factor': 1
                          };
                          """)
        logging.info("Keyspace 'worldwide_users' created successfully.")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")

def create_table(cassandra):
    try:
        cassandra.execute("""
                          CREATE TABLE IF NOT EXISTS worldwide_users.user_records (
                          uuid UUID PRIMARY KEY,
                          first_name TEXT,
                          last_name TEXT,
                          gender TEXT,
                          street TEXT,
                          city TEXT,
                          state TEXT,
                          country TEXT,
                          postcode TEXT,
                          latitude TEXT,
                          longitude TEXT,
                          email TEXT,
                          username TEXT,
                          password TEXT,
                          birthdate TEXT,
                          age TEXT,
                          registered_date TEXT,
                          phone TEXT,
                          cell TEXT,
                          picture TEXT,
                          nationality TEXT
                          );
                          """)
        logging.info("Table 'user_records' created successfully.")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")

def insert_data_into_table(cassandra, **kwargs):
    uuid= kwargs.get('uuid')
    first_name= kwargs.get('first_name')
    last_name= kwargs.get('last_name')
    gender= kwargs.get('gender')
    street= kwargs.get('street')
    city= kwargs.get('city')
    state= kwargs.get('state')
    country= kwargs.get('country')
    postcode= kwargs.get('postcode')
    latitude= kwargs.get('latitude')
    longitude= kwargs.get('longitude')
    email= kwargs.get('email')
    username= kwargs.get('username')
    password= kwargs.get('password')
    birthdate= kwargs.get('birthdate')
    age= kwargs.get('age')
    registered_date= kwargs.get('registered_date')
    phone= kwargs.get('phone')
    cell= kwargs.get('cell')
    picture= kwargs.get('picture')
    nationality= kwargs.get('nationality')
    try:
        cassandra.execute("""
                          INSERT INTO worldwide_users.user_records (
                          uuid, first_name, last_name, gender, street, city, state, country,
                          postcode, latitude, longitude, email, username, password,
                          birthdate, age, registered_date, phone, cell, picture, nationality
                          )
                          VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                          %s, %s, %s, %s, %s
                          );
                          """, (uuid, first_name, last_name, gender, street, city, state, 
                                 country,postcode, latitude, longitude, email, username,
                                 password, birthdate, age, registered_date, phone, cell,
                                 picture, nationality)
                        )
        logging.info(f"Data inserted into table successfully.")
    except Exception as e:
        logging.error(f"Failed to insert data into table: {e}")

def connect_spark_to_kafka(spark):
    try:
        stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "user_records") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
        logging.info("Connected to Kafka successfully.")
        return stream_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def format_stream_data(stream_df):
    schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField('gender', StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("age", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("nationality", StringType(), True)
    ])
    try:
        json_df =  stream_df.selectExpr("CAST(value AS STRING)")\
            .select(from_json(col("value"), schema).alias("data"))
        formatted_df = json_df.select("data.*")
        logging.info("Stream data formatted successfully.")
        return formatted_df
    except Exception as e:
        logging.error(f"Failed to format stream data: {e}")
        return None

def write_to_cassandra(batch_df, batch_id):
    try:
        if not batch_df.isEmpty():
            batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "worldwide_users") \
            .option("table", "user_records") \
            .mode("append") \
            .save()
            logging.info(f"Batch {batch_id} written to Cassandra successfully.")
        else:
            logging.info(f"Batch {batch_id} is empty, skipping write.")
    except Exception as e:
        logging.error(f"Failed to write batch {batch_id} to Cassandra: {e}")


spark_session= create_spark_session()
if spark_session is not None:
    stream_df = connect_spark_to_kafka(spark_session)
    if stream_df is not None:
        formatted_stream_df = format_stream_data(stream_df)
        if formatted_stream_df is not None:
            cassandra_session= connect_to_cassandra()
            if cassandra_session is not None:
                create_keyspace(cassandra_session)
                create_table(cassandra_session)
                query = formatted_stream_df.writeStream \
                    .foreachBatch(write_to_cassandra) \
                    .option("checkpointLocation", "/tmp/spark-kafka-checkpoint") \
                    .start()
                query.awaitTermination()
                logging.info("Streaming query started successfully.")

    
