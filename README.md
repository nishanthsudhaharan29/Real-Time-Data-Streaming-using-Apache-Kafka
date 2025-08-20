## Real-Time Data Streaming with Apache Airflow, Apache Kafka, and Apache Spark
![System Architecture](./Architecture.PNG)

# Real-Time User Data Streaming Pipeline with Kafka, Spark, Cassandra & Airflow

This project demonstrates the implementation of a **real-time data streaming pipeline** using **Apache Kafka, Apache Spark, Apache Airflow, Cassandra, Docker, and Schema Registry**. It fetches live user data from an external API, processes it in real-time, and stores it in **Cassandra** for analytical and operational use. The solution follows a fully containerized architecture built using **Docker Compose**.

---

## **Project Overview**

The project automates the ingestion, transformation, streaming, and storage of real-time user data using modern data engineering tools.

- **Data Ingestion** → Fetches random user data from an API.
- **Streaming Pipeline** → Uses Kafka for event streaming.
- **Data Transformation** → Processes data using Apache Spark Structured Streaming.
- **Data Storage** → Stores enriched data into Apache Cassandra for fast querying.
- **Workflow Orchestration** → Apache Airflow manages end-to-end pipeline execution.
- **Monitoring & Schema Management** → Uses Control Center and Schema Registry for Kafka topic and schema management.

---

## **Solution Architecture**

### **Components in the Architecture**

| Component        | Technology Used            | Purpose                                |
|------------------|---------------------------|---------------------------------------|
| API             | Random User API          | Source of real-time data             |
| Airflow         | Apache Airflow          | Orchestrates streaming jobs         |
| Kafka           | Apache Kafka           | Real-time event streaming          |
| Zookeeper      | Apache Zookeeper      | Kafka cluster coordination         |
| Schema Registry | Confluent Schema Registry | Manages schema evolution           |
| Control Center  | Confluent Control Center | Kafka monitoring dashboard        |
| Spark          | Apache Spark Structured Streaming | Processes data streams |
| Cassandra     | Apache Cassandra    | Stores processed data |
| Docker       | Docker Compose   | Containerizes the entire solution |
| PostgreSQL | PostgreSQL | Metadata DB for Airflow |

---

## **Workflow Explanation**

### **1. Data Ingestion**
- Data is fetched from **RandomUser API** using a Python producer script.
- The producer sends user data to the Kafka topic **`user_records`**.
- Data is serialized as **JSON** before sending.

### **2. Kafka Streaming Layer**
- Kafka brokers handle **high-throughput, low-latency** event streaming.
- Zookeeper manages Kafka broker coordination.
- The Schema Registry ensures **consistent message formats**.
- Confluent Control Center provides a **monitoring dashboard** for topics, partitions, and offsets.

### **3. Real-Time Processing with Spark**
- Spark Structured Streaming consumes events from Kafka in **micro-batches**.
- Data is parsed and transformed into a **structured schema**.
- Validated records are written directly to **Cassandra** in real-time.

### **4. Cassandra Storage Layer**
- Processed records are stored in the **`worldwide_users.user_records`** table.
- Optimized for **fast writes** and **low-latency reads**.

### **5. Workflow Orchestration with Airflow**
- Airflow DAG schedules and manages the **entire pipeline**.
- Uses the **`SparkSubmitOperator`** to trigger the Spark Structured Streaming job.
- Stores task metadata in **PostgreSQL**.
- Supports retry policies, failure handling, and manual triggers.

---

## **Technology Stack**

| Layer            | Technology                           |
|------------------|-------------------------------------|
| Data Source      | RandomUser API                       |
| Orchestration    | Apache Airflow                       |
| Streaming        | Apache Kafka, Zookeeper             |
| Schema Management| Confluent Schema Registry          |
| Monitoring       | Confluent Control Center            |
| Processing       | Apache Spark Structured Streaming  |
| Storage          | Apache Cassandra                    |
| Containerization | Docker Compose                      |
| Metadata DB      | PostgreSQL                          |

---

## **Repository Structure**

```bash
 Real-time-User-Streaming-Pipeline
├── dags/                  # Airflow DAG scripts
├── scripts/               # Spark, Kafka, and API producer scripts
├── airflow_build/         # Custom Airflow image setup
├── cassandra-conf/        # Cassandra configurations
├── docker-compose.yml     # Full containerized environment setup
├── requirements.txt       # Python dependencies
├── architecture.png       # System architecture diagram
└── README.md              # Project documentation
