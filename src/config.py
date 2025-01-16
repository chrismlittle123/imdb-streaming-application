from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    ArrayType,
)


class Config:
    """Configuration settings for the IMDB data analysis application"""

    # S3 bucket configuration
    S3_BUCKET = "imdb-data-495700631743"
    AWS_REGION = "eu-west-2"
    S3_PREFIX = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com"

    # File paths
    RATINGS_PATH = f"{S3_PREFIX}/title.ratings.parquet"
    BASICS_PATH = f"{S3_PREFIX}/title.basics.parquet"
    CREW_PATH = f"{S3_PREFIX}/title.crew.parquet"
    EPISODE_PATH = f"{S3_PREFIX}/title.episode.parquet"
    NAMES_PATH = f"{S3_PREFIX}/name.basics.parquet"
    PRINCIPALS_PATH = f"{S3_PREFIX}/title.principals.parquet"

    # Local data directory
    DATA_DIR = "data"

    # Constants
    MIN_VOTES = 500  # Minimum number of votes required for movie consideration
    BATCH_SIZE = 100  # Number of records per batch for streaming

    # Schema definitions
    RATINGS_SCHEMA = StructType(
        [
            StructField("tconst", StringType(), False),
            StructField("averageRating", FloatType(), False),
            StructField("numVotes", IntegerType(), False),
        ]
    )

    MOVIES_SCHEMA = StructType(
        [
            StructField("tconst", StringType(), False),
            StructField("titleType", StringType(), False),
            StructField("primaryTitle", StringType(), False),
            StructField("originalTitle", StringType(), False),
            StructField("isAdult", BooleanType(), False),
            StructField("startYear", IntegerType(), True),
            StructField("endYear", IntegerType(), True),
            StructField("runtimeMinutes", IntegerType(), True),
            StructField("genres", StringType(), True),
        ]
    )

    NAMES_SCHEMA = StructType(
        [
            StructField("nconst", StringType(), False),
            StructField("primaryName", StringType(), False),
            StructField("birthYear", IntegerType(), True),
            StructField("deathYear", IntegerType(), True),
            StructField(
                "primaryProfession", StringType(), True
            ),  # Comma-separated list
            StructField("knownForTitles", StringType(), True),  # Comma-separated list
        ]
    )

    CREW_SCHEMA = StructType(
        [
            StructField("tconst", StringType(), False),
            StructField("directors", StringType(), True),  # Comma-separated list
            StructField("writers", StringType(), True),  # Comma-separated list
        ]
    )

    EPISODE_SCHEMA = StructType(
        [
            StructField("tconst", StringType(), False),
            StructField("parentTconst", StringType(), False),
            StructField("seasonNumber", IntegerType(), True),
            StructField("episodeNumber", IntegerType(), True),
        ]
    )

    PRINCIPALS_SCHEMA = StructType(
        [
            StructField("tconst", StringType(), False),
            StructField("ordering", IntegerType(), False),
            StructField("nconst", StringType(), False),
            StructField("category", StringType(), True),
            StructField("job", StringType(), True),
            StructField("characters", StringType(), True),  # JSON array as string
        ]
    )

    # Spark Configuration
    SPARK_CONFIGS = {
        # General Spark configs
        "spark.sql.streaming.schemaInference": "true",
        "spark.sql.streaming.checkpointLocation": "./checkpoints",
        "spark.sql.shuffle.partitions": "10",
        "spark.driver.host": "spark",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.network.timeout": "600s",
        # Memory configurations
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        # Streaming configurations
        "spark.streaming.stopGracefullyOnShutdown": "true",
        "spark.streaming.backpressure.enabled": "true",
    }

    # Streaming Configuration
    STREAM_TRIGGER_INTERVAL = "30 seconds"
    CHECKPOINT_LOCATION = "./checkpoints"

    # Output Configuration
    OUTPUT_MODES = {
        "console": "complete",  # Show all results each time
        "memory": "complete",  # Keep full results in memory
        "file": "append",  # Append new results to files
    }

    # Partition configurations
    RATINGS_VOTE_RANGES = [
        (500, 1000),
        (1001, 5000),
        (5001, 10000),
        (10001, float("inf")),
    ]
