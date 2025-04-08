# 🛠️ Airflow + PySpark + PostgreSQL Pipeline with Astronomer CLI

This project sets up an end-to-end batch data pipeline using **Apache Airflow**, **Apache Spark**, and **PostgreSQL** — containerized with **Docker** and managed via **Astronomer CLI**.

---

## 🚀 Tech Stack

- **Airflow**: Orchestrates the pipeline  
- **Apache Spark**: Reads and transforms batch data  
- **PostgreSQL**: Stores the transformed data  
- **Docker + Astronomer CLI**: Simplifies setup and orchestration  

---

## ⚙️ Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli)

---

## 🔧 Start the Project

Use Astronomer CLI to spin up the Docker containers:

```bash
astro dev start
```
This will build and start:

- Airflow webserver
- Airflow scheduler
- Spark-master
- Spark-worker
- PostgreSQL

To shut everything down:
```bash
astro dev stop
```
To clean up everything (remove containers, networks, etc.):
```bash
astro dev stop && docker system prune -af
```
---

## 🌐 Access Airflow UI
- Visit: http://localhost:8080
- Default credentials:
    - Username: admin
    - Password: admin
---
## 🔌 Add Airflow Connections
Go to Airflow UI → Admin → Connections, then add:
- ➤ Spark Connection
    - Conn Id: my_spark_conn
    - Conn Type: Spark
    - Host: spark://spark-master:7077
- ➤ PostgreSQL Connection
    - Conn Id: my_postgres_conn
    - Conn Type: Postgres
    - Host: postgres
    - Schema: my_db
    - Login: postgres
    - Password: postgres
    - Port: 5432 (container port)
---

## 📋 Example DAG Flow
- Read CSV data using PySpark
- Perform transformations like:
    - Region-wise total sales
    - Product category aggregations
- Save transformed data to PostgreSQL
---
## 🗃️ PostgreSQL Volume & Data Persistence
PostgreSQL uses a Docker volume to persist data:

```yaml
volumes:
  - ./pg-data:/var/lib/postgresql/data
```
To access PostgreSQL from terminal:
```bash
docker exec -it <postgres_container_id> psql -U postgres -d my_db
```
Inside the psql shell:

```sql
\dt
SELECT * FROM Products;
```
