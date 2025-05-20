from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, when
from pyspark.sql import functions as F

import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    
    def app():
        try:
            logger.info("Starting Spark session")
            spark = SparkSession.builder.appName("FormatJson") \
                .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
                .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
                .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
                .config("fs.s3a.connection.ssl.enabled", "false") \
                .config("fs.s3a.path.style.access", "true") \
                .config("fs.s3a.attempts.maximum", "1") \
                .config("fs.s3a.connection.establish.timeout", "5000") \
                .config("fs.s3a.connection.timeout", "10000") \
                .getOrCreate()
            
            bucket_name = os.getenv("SPARK_APPLICATION_ARGS")
            category = os.getenv("FOLDER_NAME")
            date = os.getenv("DAG_RUN_DATE")
            input_json_path = f's3a://{bucket_name}/{date}/{category}/{category}.json'
            
            try:
                df = spark.read.json(input_json_path)
                logger.info("JSON file read successfully.")
            except Exception as e:
                logger.error("Failed to read json file.")

            if category == 'population_trend':
                formatted_df = df.select(
                    col("PRD_DE").cast("int").alias("date"),
                    col("DT").cast("float").alias("value"),
                    col("PRD_SE").alias("gender").cast("string"),
                    when(col("C1_NM_ENG") == "Whole country", "Total") \
                        .otherwise(col("C1_NM_ENG").cast("string")) \
                        .alias("country"),
                    col("C2_NM").cast("string").alias("category")
                )
                formatted_df = formatted_df.withColumn("trend_id", concat(
                    col("date").cast("string"),
                    lit("_"),
                    col("country"),
                    lit("_"),
                    col("category")
                ))
            elif category == 'average_first_marriage_age':
                formatted_df = df.select(
                    col("PRD_DE").cast("int").alias("date"),
                    when(col("ITM_NM_ENG") == "Husband", "M") \
                        .when(col("ITM_NM_ENG") == "Wife", "F") \
                        .otherwise(col("ITM_NM_ENG").cast("string")).alias("gender"),
                    col("DT").cast("int").alias("age"),
                    when(col("C1_NM_ENG") == "Whole country", "Total") \
                        .otherwise(col("C1_NM_ENG").cast("string")).alias("country"),
                )
                formatted_df = formatted_df.withColumn("age_id", concat(
                    col("date").cast("string"),
                    lit("_"),
                    col("gender"),
                    lit("_"),
                    col("country")
                ))
            elif category == 'gender_income':
                formatted_df = df.select(
                    col("PRD_DE").cast("int").alias("date"),
                    col("ITM_NM_ENG").cast("string").alias("category"),
                    col("UNIT_NM").cast("string").alias("unit"),
                    col("C1_NM").cast("string").alias("employment_type"),
                    when(col("C2_NM_ENG") == "Male", "M") \
                        .when(col("C2_NM_ENG") == "Female", "F") \
                        .otherwise(col("C2_NM_ENG").cast("string")).alias("gender"),
                    col("DT").cast("float").alias("value")
                )
                formatted_df = formatted_df.withColumn("category", F.regexp_replace(
                    F.regexp_replace(F.regexp_replace(col("category"), " ", "_"), "/", "_"), "&", "_"
                ))
                formatted_df = formatted_df.withColumn("income_id", concat(
                    col("date").cast("string"),
                    lit("_"),
                    col("category"),
                    lit('_'),
                    col("gender"),
                    lit("_"),
                    col("employment_type")
                ))
            
            output_csv_path = f's3a://{bucket_name}/{date}/{category}/csv'
            logger.info(f"Saving formatted data to: {output_csv_path}")
            
            formatted_df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_csv_path)
            logger.info("Formatted data saved successfully.")
        
            
        except Exception as e:
            logger.error("An error occurred:", exc_info=True)

    app()
    os.system('kill %d' % os.getpid())