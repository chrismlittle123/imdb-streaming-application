from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
)


class Config:
    """Configuration settings for the IMDB data analysis application"""

    # S3 bucket configuration
    S3_BUCKET = "imdb-data-495700631743"
    AWS_REGION = "eu-west-2"
    S3_PREFIX = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com"

    # File paths
    RATINGS_PATH = f"{S3_PREFIX}/title.ratings.tsv"
    BASICS_PATH = f"{S3_PREFIX}/title.basics.tsv"
    CREW_PATH = f"{S3_PREFIX}/title.crew.tsv"
    EPISODE_PATH = f"{S3_PREFIX}/title.episode.tsv"
    NAMES_PATH = f"{S3_PREFIX}/name.basics.tsv"
    PRINCIPALS_PATH = f"{S3_PREFIX}/title.principals.tsv"
    AKAS_PATH = f"{S3_PREFIX}/title.akas.tsv"

    # Constants
    MIN_VOTES = 500  # Minimum number of votes required for movie consideration

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
            StructField("primaryProfession", StringType(), True),
            StructField("knownForTitles", StringType(), True),
        ]
    )

    CREW_SCHEMA = StructType(
        [
            StructField("tconst", StringType(), False),
            StructField("directors", StringType(), True),
            StructField("writers", StringType(), True),
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
            StructField("characters", StringType(), True),
        ]
    )

    AKAS_SCHEMA = StructType(
        [
            StructField("titleId", StringType(), False),
            StructField("ordering", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("region", StringType(), True),
            StructField("language", StringType(), True),
            StructField("types", StringType(), True),
            StructField("attributes", StringType(), True),
            StructField("isOriginalTitle", BooleanType(), True),
        ]
    )

    # Spark Configuration
    SPARK_CONFIGS = {
        "spark.sql.streaming.schemaInference": "true",
        "spark.sql.streaming.checkpointLocation": "./checkpoints",
        "spark.sql.shuffle.partitions": "10",
        "spark.driver.host": "spark",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.network.timeout": "600s",
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
