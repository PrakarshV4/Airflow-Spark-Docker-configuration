# my_dag.py
from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
    
@dag(
    start_date=datetime(2025, 4, 8),
    schedule=None,
    catchup=False,
)
def my_dag():
    
    @task.pyspark(conn_id="my_spark_conn")
    def read_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        schema = StructType([
            StructField("date", StringType(), True),         # Could also use DateType() if you're parsing it later
            StructField("product", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_sales", DoubleType(), True),
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True),
        ])
        df = spark.read.option("header", True).schema(schema).csv('./include/data.csv')
        df.show()
        return df.toPandas()
    
    @task.pyspark(conn_id="my_spark_conn")
    def transformation_on_data(data_df: pd.DataFrame, spark: SparkSession, sc: SparkContext):
        df = spark.createDataFrame(data_df)
        region_wise_sales_df = df.groupBy("region").agg(sum("total_sales").alias("total_region_sales"))
        return region_wise_sales_df.toPandas()

    @task
    def write_data_to_postgres(df: pd.DataFrame) -> None:
        conn = BaseHook.get_connection("my_postgres_conn")  # <-- set this ID in Airflow UI
        engine = create_engine(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )
        
        df.to_sql("region_wise_sales", engine, index=False, if_exists="replace")
        print("✅ Data written to PostgreSQL table 'region_wise_sales'")

    read_df = read_data()
    transformed_df = transformation_on_data(read_df)
    write_data_to_postgres(transformed_df)


my_dag()