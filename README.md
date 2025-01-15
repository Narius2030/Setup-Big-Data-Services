# Setup Big Data Services
This repository streamlines the deployment of a full-fledged Big Data stack using configuration files and installation scripts. It includes configurations for popular tools such as:
* [Apache Spark](#apache-spark): Supporting distributed data processing. 
* [Spark Notebook](#spark-notebook):  Interactive data analysis and visualization with Spark Cluster. 
* [Apache Kafka](#apache-kafka): A messaging queue tool for streaming data. 
* Apache Airflow: Scheduling the tasks for data pipeline.
* MinIO: An object storage which associating with S3. 
* [Hive Metastore](#hive-metastore): Using metadata management by Hive abstraction layer which support Spark in processing.
* [Spark Thrift Server](#spark-thrift-server): Provides an interactive interface for executing SQL queries on Spark, especially for external applications using JDBC or ODBC.

This project simplifies the often complex setup process, allowing users to focus on data analysis and development rather than infrastructure management. It provides a consistent and reproducible environment for Big Data projects.


## Apache Spark
**1. Spark Master**

This container runs the **Spark Master**, which acts as the central manager of the Spark cluster.
-   **Build from Dockerfile**:
    -   The Dockerfile is located in the `./spark` directory and is used to build the container.
-   **Container Name**: `spark-master`.
-   **Ports**:
    -   `7077`: The main port for communication with Spark Workers.
    -   `8082`: The web interface (Spark Master Web UI) port for monitoring the cluster.
-   **Environment Variables**:
    -   **`SPARK_MODE=master`**: Specifies that the container runs in Master mode.
    -   Other variables (e.g., `SPARK_RPC_AUTHENTICATION_ENABLED=no`) disable security features like RPC encryption or SSL.
    -   **`SPARK_USER=spark`**: Runs Spark with the user `spark`.
-   **Volumes**:
    -   **`spark-defaults.conf`**: Configuration file for Spark (e.g., default settings like `spark.master`, `executor.memory`).
    -   **`log4j.properties`**: Log configuration file (e.g., Spark log levels)

**2. Spark Worker**

This container runs a **Spark Worker**, responsible for executing tasks assigned by the Master.
-   **Image**: Uses the official Bitnami Spark image (`docker.io/bitnami/spark:3.3.2`).
-   **Container Name**: `spark-worker-1`.
-   **Dependency**: Ensures the Spark Worker only starts after the Spark Master has initialized (**`depends_on: spark-master`**).
-   **Environment Variables**:
    -   Loaded from the `.env` file (not shown here). Typically includes settings such as:
        -   The address of the Spark Master (`spark://spark-master:7077`).
        -   Memory or core allocations for the Worker.


## Spark Notebook

This container provides an interactive environment for working with Spark and MinIO through Jupyter Notebook or JupyterLab.

-   **Build from Dockerfile**:
    -   The Dockerfile is located in the `./notebook` directory and is used to build the container.
-   **Container Name**: `spark-notebook`.
-   **User**: Runs as the root user to allow administrative privileges.
-   **Environment Variables**:
    -   **`JUPYTER_ENABLE_LAB="yes"`**: Enables JupyterLab as the default interface.
    -   **`GRANT_SUDO="yes"`**: Allows the user to run commands with `sudo` privileges.
    -   **`MLFLOW_S3_ENDPOINT_URL=http://minio:9000/`**: Configures MLflow to use MinIO for storing artifacts (compatible with the S3 protocol).
    -   **`AWS_ACCESS_KEY_ID`** and **`AWS_SECRET_ACCESS_KEY`**: Authentication credentials for accessing MinIO.
-   **Volumes**:
    -   **`./notebook/work:/home/nhanbui/work`**: Maps the local `./notebook/work` directory to `/home/nhanbui/work` inside the container, providing a workspace for storing notebooks and data.
    -   **`./notebook/conf/spark-defaults.conf:/usr/local/spark/conf/spark-defaults.conf`**: Mounts the Spark configuration file to set Spark runtime parameters.
-   **Ports**:
    -   **`8888:8888`**: Exposes the Jupyter Notebook/Lab interface on port 8888.
    -   **`4040:4040`**: Exposes the Spark Web UI port (for monitoring Spark jobs).

## Spark Thrift Server

This container runs the **Spark Thrift Server**, allowing clients to execute SQL queries on Spark using JDBC/ODBC.
-   **Build from Dockerfile**:
    -   The Dockerfile is located in the `./spark` directory and is used to build the container.
-   **Container Name**: `spark-thrift-server`.
-   **Restart Policy**: Set to `always`, ensuring the container restarts automatically if it stops unexpectedly.
-   **Dependencies**:
    -   **`spark-master`**: The Thrift Server depends on the Spark Master to execute distributed tasks.
    -   **`hive-metastore`**: The Thrift Server connects to Hive Metastore for schema and metadata management.
-   **Ports**:
    -   **`4041:4040`**: Exposes the Spark Web UI for monitoring jobs submitted to the Thrift Server.
    -   **`10000:10000`**: Exposes the JDBC/ODBC connection port for clients to interact with the Thrift Server.
-   **Command**:
    -   Runs the Thrift Server with specific configurations:
        -   Connects to Hive Metastore using the URI `thrift://hive-metastore:9083`.
        -   Configures Spark Master as `spark://spark-master:7077`.
        -   Allocates **1 GB driver memory**, **1 GB executor memory**, and **1 core** for executors.
        -   Adds a 10-second delay (`sleep 10`) to ensure dependencies are fully initialized before starting the server.
-   **Volumes**:
    -   **`spark-defaults.conf`**: Provides Spark runtime configurations.
    -   **`hive-site.xml`**: Configures Hive-specific properties (e.g., Hive Metastore URI).

## Hive Metastore
**1. MariaDB**

The MariaDB container serves as the metadata database for the Hive Metastore.
-   **Image**: `mariadb:10.5.16` – Uses version 10.5.16 of MariaDB.
-   **Container Name**: `mariadb`.
-   **Volumes**:
    -   **`./mariadb:/var/lib/mysql`**: Maps the host directory `./mariadb` to the container's MariaDB data directory. This ensures the database persists even if the container is removed.
-   **Ports**:
    -   **`3309:3306`**: Maps port 3306 (MariaDB's default port) to port 3309 on the host machine, allowing external connections.
-   **Environment Variables**:
    -   Specified in the `.env` file to configure database credentials and settings.

The Hive Metastore container provides a centralized metadata service for managing schemas, table definitions, and storage locations in a distributed data system.

**2. Hive Metastore**

-   **Image**: `bitsondatadev/hive-metastore:latest` – Uses the latest version of the Hive Metastore image.
-   **Container Name**: `hive-metastore`.
-   **Hostname**: Set as `hive-metastore` for consistent naming in the network.
-   **Entry Point**:
    -   Specifies `/entrypoint.sh` as the script to initialize the container.
-   **Ports**:
    -   **`9083:9083`**: Exposes the Hive Metastore service on port 9083 for communication with Hive-compatible tools like Spark.
-   **Volumes**:
    -   **`./hive-metastore/metastore-site.xml:/opt/apache-hive-metastore-3.0.0-bin/conf/metastore-site.xml`**:
        -   Maps the `metastore-site.xml` configuration file from the host to the container.
        -   This file specifies Hive Metastore settings, including the connection details for MariaDB.
-   **Environment Variables**:
    -   **`METASTORE_DB_HOSTNAME=mariadb`**: Configures the Hive Metastore to use the `mariadb` container as its metadata database.
-   **Dependencies**:
    -   **`depends_on: mariadb`**: Ensures the MariaDB container starts before the Hive Metastore.

## Apache Kafka

 **1. Zookeeper**

-   **Purpose**: Manages metadata and coordination for Kafka brokers (e.g., leader election and configuration management).
-   **Image**: `confluentinc/cp-zookeeper:5.4.0`.
-   **Ports**:
    -   Exposes port `2181` for client and broker communication.
-   **Environment Variables**:
    -   Configures the client port (`2181`) and tick time (`2000 ms` for heartbeats).

**2. Broker**

-   **Purpose**: Kafka broker that stores and serves data streams.
-   **Image**: `confluentinc/cp-server:5.4.0`.
-   **Depends On**: Zookeeper, which manages broker metadata.
-   **Ports**:
    -   Exposes `9092` for external client communication.
-   **Environment Variables**:
    -   Configures broker ID, Zookeeper connection, advertised listeners, replication factors, and metrics reporting.
    -   Supports both internal (`29092`) and external (`9092`) connections.

**3. Kafka Tools**

-   **Purpose**: Utility container for managing Kafka (e.g., producing/consuming messages or managing topics).
-   **Image**: `confluentinc/cp-kafka:5.4.0`.
-   **Command**: Runs idle (`tail -f /dev/null`) until manual commands are executed.
-   **Network Mode**: Shares the host network for direct access to Kafka.

 **4. Schema Registry**

-   **Purpose**: Manages schemas for Kafka messages, ensuring compatibility and versioning.
-   **Image**: `confluentinc/cp-schema-registry:5.4.0`.
-   **Depends On**: Zookeeper and Broker.
-   **Ports**:
    -   Exposes `8081` for clients to interact with the Schema Registry.
-   **Environment Variables**:
    -   Configures Zookeeper for Kafka storage and the Schema Registry's hostname.

 **5. Control Center**

-   **Purpose**: Provides a web-based UI to monitor and manage the Kafka ecosystem (e.g., topics, producers, consumers, and metrics).
-   **Image**: `confluentinc/cp-enterprise-control-center:5.4.0`.
-   **Depends On**: Zookeeper, Broker, and Schema Registry.
-   **Ports**:
    -   Exposes `9021` for the web UI.
-   **Environment Variables**:
    -   Configures connections to the Kafka broker, Zookeeper, and Schema Registry.
    -   Sets replication factors and topic partitions for monitoring.
