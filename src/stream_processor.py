from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, expr, lit, current_timestamp, count
from typing import Optional
import os
import requests

from src.config import Config


def download_file(url: str, local_path: str) -> None:
    """Download a file from URL to local path"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


class StreamProcessor:
    def __init__(self, spark: SparkSession):
        """Initialize the stream processor with a Spark session"""
        self.spark = spark
        self._setup_spark_session()
        self.avg_num_votes: Optional[float] = None

    def _setup_spark_session(self):
        """Apply any additional Spark configurations"""
        # Only set truly mutable configurations
        mutable_configs = {
            "spark.sql.shuffle.partitions": "10",
        }
        for key, value in mutable_configs.items():
            self.spark.conf.set(key, value)

    def _calculate_average_votes(self, ratings_df: DataFrame) -> float:
        """Calculate the average number of votes across all movies"""
        return ratings_df.agg(avg("numVotes")).collect()[0][0]

    def _calculate_ranking(self, df: DataFrame) -> DataFrame:
        """Calculate the ranking metric for each movie"""
        return df.withColumn(
            "ranking",
            (col("numVotes") / lit(self.avg_num_votes)) * col("averageRating"),
        )

    def _ensure_local_file(self, url: str, filename: str) -> str:
        """Ensure file exists locally, downloading if necessary"""
        local_path = os.path.join(Config.DATA_DIR, filename)
        if not os.path.exists(local_path):
            print(f"Downloading {filename} from {url}...")
            download_file(url, local_path)
        return local_path

    def process_ratings(self):
        """Process ratings data in batches and maintain top 10 movies"""
        # Ensure we have the required files locally
        local_ratings_path = self._ensure_local_file(
            Config.RATINGS_PATH, "title.ratings.tsv"
        )
        local_basics_path = self._ensure_local_file(
            Config.BASICS_PATH, "title.basics.tsv"
        )

        # Read the ratings data to calculate average votes
        print("Reading ratings data to calculate average votes...")
        ratings_df = self.spark.read.csv(
            local_ratings_path,
            schema=Config.RATINGS_SCHEMA,
            sep=r"\t",
            header=True,
        )

        # Read the basics data for movie titles
        print("Reading movie titles data...")
        basics_df = self.spark.read.csv(
            local_basics_path,
            schema=Config.MOVIES_SCHEMA,
            sep=r"\t",
            header=True,
        ).filter(
            col("titleType") == "movie"
        )  # Filter for movies only

        # Calculate average votes before starting the stream
        self.avg_num_votes = self._calculate_average_votes(ratings_df)
        print(f"Average number of votes: {self.avg_num_votes}")

        # Filter and calculate rankings for all movies
        print("\nCalculating rankings for all movies...")
        processed_df = (
            ratings_df.join(
                basics_df.select("tconst"), "tconst", "inner"
            )  # Only keep movies
            .filter(col("numVotes") >= Config.MIN_VOTES)
            .withColumn(
                "ranking",
                (col("numVotes") / lit(self.avg_num_votes)) * col("averageRating"),
            )
        )

        # Get top 10 movies sorted by ranking from highest to lowest
        top_movies_df = processed_df.orderBy(col("ranking").desc()).limit(10)

        # Join with basics to get movie titles
        top_movies_with_titles = top_movies_df.join(basics_df, "tconst", "left").select(
            "tconst",
            "primaryTitle",
            "originalTitle",
            "titleType",
            "startYear",
            "averageRating",
            "numVotes",
            "ranking",
        )

        # Process in batches
        batch_size = Config.BATCH_SIZE
        total_rows = ratings_df.count()
        print(f"\nProcessing {total_rows} records in batches of {batch_size}...")

        # Show the top 10 movies with titles, sorted by ranking
        print("\nTop 10 Movies by Ranking:")
        print("=" * 120)
        top_movies_with_titles.orderBy(col("ranking").desc()).show(truncate=False)

    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()


def create_spark_session() -> SparkSession:
    """Create and configure a Spark session"""
    return (
        SparkSession.builder.appName("IMDB Streaming Analysis")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )


if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()

    try:
        # Initialize and run the stream processor
        processor = StreamProcessor(spark)
        processor.process_ratings()
    except KeyboardInterrupt:
        print("\nStopping the streaming process...")
    finally:
        # Clean up
        processor.stop()
