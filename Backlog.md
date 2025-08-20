## Project Backlog: Real-Time Streaming Data Pipeline with Apache Airflow, Apache Kafka, and Apache Spark

| ID | Epic                          | User Story                                                                 | Estimate Hours | Status |
|----|-------------------------------|----------------------------------------------------------------------------|----------------|--------|
| 1  | Environment Setup             | Set up Docker for containerizing the entire environment                    | 2              | Completed       |
|    |                               | Create Docker Compose file for orchestration                               | 2              |  Completed      |
|    |                               | Pull/setup images for Airflow, Kafka, Zookeeper, Spark, Cassandra         | 1.5            |  Completed      |
| 2  | Data Ingestion with Airflow   | Configure DAG to ingest data from public API                               | 2              |  Completed      |
|    |                               | Store intermediate results to PostgreSQL                                   | 1              |  Completed      |
|    |                               | Push data from Airflow to Kafka topic                                      | 1.5            | Completed       |
| 3  | Kafka Pipeline                | Set up Kafka topics and partitions                                         | 1              | Completed       |
|    |                               | Connect Kafka with Schema Registry for message validation                  | 1              | Completed       |
|    |                               | Monitor topics using Kafka Control Center                                  | 1              |  Completed      |
|    |                               | Configure Zookeeper for Kafka coordination                                 | 1              |Completed        |
| 4  | Stream Processing with Spark  | Set up Spark Master and Worker nodes                                       | 1.5            |  Completed      |
|    |                               | Create Spark Streaming job to consume from Kafka                           | 2.5            |  Completed      |
|    |                               | Process data (e.g., transform, clean, filter) in Spark                     | 2              |  Completed      |
| 5  | Data Storage with Cassandra   | Create keyspaces and tables in Cassandra                                   | 1              |  Completed      |
|    |                               | Configure Spark to write streaming results into Cassandra                  | 1.5            |  Completed      |
|    |                               | Verify real-time data write consistency in Cassandra                       | 1              | Completed       |
| 6  | Validation & Monitoring       | End-to-end test: API → Airflow → Kafka → Spark → Cassandra                 | 2              |   Completed     |
|    |                               | Implement logging and error handling across all components                 | 1.5            |  Completed      |
|    |                               | Monitor pipeline health using Control Center and logs                      | 1              |   Completed     |
