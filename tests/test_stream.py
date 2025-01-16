import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
import tempfile

from src.stream_processor import StreamProcessor
from src.config import Config


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return (
        SparkSession.builder.appName("TestStreamProcessor")
        .master("local[*]")
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def sample_ratings_data(spark):
    """Create sample ratings data"""
    data = [
        ("tt0000001", 5.6, 1500),
        ("tt0000002", 6.0, 2000),
        ("tt0000003", 7.0, 3000),
        ("tt0000004", 8.0, 4000),
        ("tt0000005", 9.0, 5000),
    ]
    return spark.createDataFrame(data, schema=Config.RATINGS_SCHEMA)


def test_stream_processor_initialization(spark):
    """Test StreamProcessor initialization"""
    processor = StreamProcessor(spark)
    assert processor.spark == spark
    assert processor.avg_num_votes is None


def test_calculate_average_votes(spark, sample_ratings_data):
    """Test average votes calculation"""
    processor = StreamProcessor(spark)
    avg_votes = processor._calculate_average_votes(sample_ratings_data)
    assert avg_votes == 3100.0  # (1500 + 2000 + 3000 + 4000 + 5000) / 5


def test_calculate_ranking(spark, sample_ratings_data):
    """Test ranking calculation"""
    processor = StreamProcessor(spark)

    # Calculate rankings
    ranked_df = processor._calculate_ranking(sample_ratings_data)

    # Get the results as a list of rows
    results = ranked_df.collect()

    # Verify the ranking calculation for the first row
    first_row = results[0]
    expected_ranking = (first_row.numVotes / 3100.0) * first_row.averageRating
    assert abs(first_row.ranking - expected_ranking) < 0.001


def test_minimum_votes_filter(spark, sample_ratings_data):
    """Test filtering by minimum votes"""
    processor = StreamProcessor(spark)

    # Add some data below minimum votes threshold
    data_with_low_votes = [
        ("tt0000006", 9.0, 100),  # Below minimum
        ("tt0000007", 8.0, 400),  # Below minimum
    ]
    test_df = spark.createDataFrame(
        sample_ratings_data.collect() + data_with_low_votes,
        schema=Config.RATINGS_SCHEMA,
    )

    # Filter and calculate rankings
    filtered_df = test_df.filter(test_df.numVotes >= Config.MIN_VOTES)

    # Verify only records with sufficient votes remain
    assert filtered_df.count() == 5  # Original sample data only
    assert all(row.numVotes >= Config.MIN_VOTES for row in filtered_df.collect())
