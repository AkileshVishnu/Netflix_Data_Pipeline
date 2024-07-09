# Netflix Data Pipeline

## Project Overview

The **Netflix Data Pipeline** project aims to provide a comprehensive understanding of the Data Build Tool (DBT) within an ETL framework. By leveraging DBT, Snowflake, and Airflow, the project constructs a robust ETL pipeline using Docker for containerization. Additionally, the project incorporates Slack and email notifications using SNS to ensure efficient monitoring.

## Tech Stack
- **Languages**: Python, SQL
- **Tools**: DBT
- **Database**: Snowflake
- **Services**: Docker, Airflow

## Step-by-Step Execution

### Step 1: Environment Setup
- Install Python
- Create a project directory and virtual environment
- Install necessary libraries via `requirements.txt`
- Set up Airflow with Docker
- Create Snowflake and DBT accounts

### Step 2: Airflow Setup with Docker
- Install Docker Desktop and integrate with WSL
- Configure `docker-compose.yml` for Airflow services
- Build custom Docker image for Airflow with dependencies

### Step 3: DBT-Snowflake Integration
- Install `dbt-snowflake`
- Configure `profiles.yml` for Snowflake credentials
- Initialize DBT project and configure `dbt_project.yml`
- Test DBT connection with `dbt debug`

### Step 4: Data Modeling with DBT
- Create models for business requirements, such as `popularity_dim.sql`
- Run DBT models and materialize in Snowflake

### Step 5: Adding DBT Models to Airflow DAG
- Mount DBT folders in Docker
- Define DBT tasks in Airflow DAG
- Trigger DAG to ensure tasks execute properly

### Step 6: Error and Success Handling
- Integrate Slack for real-time notifications
- Configure Airflow callbacks for task success and failure alerts

## Conclusion

The Netflix Data Pipeline project demonstrates a powerful and scalable ETL framework using modern data tools and best practices in containerization and orchestration. By successfully integrating DBT, Snowflake, Airflow, and Slack, the project ensures efficient data processing, monitoring, and error handling, providing valuable insights and supporting data-driven decision-making.
