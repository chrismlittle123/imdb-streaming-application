import os
import pytest
import requests
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from src.config import Config


def download_file(url, local_path):
    """Download a file from URL to local path"""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return (
        SparkSession.builder.appName("TestIMDBAnalysis")
        .master("local[*]")
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def temp_dir():
    """Create a temporary directory for downloaded files"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture(scope="module", autouse=True)
def cleanup_spark():
    """Clean up Spark session after tests"""
    yield
    SparkSession.builder.getOrCreate().stop()


def test_s3_paths():
    """Test that S3 paths are properly configured"""
    assert Config.S3_BUCKET == "imdb-data-495700631743"
    assert Config.AWS_REGION == "eu-west-2"
    assert (
        Config.S3_PREFIX
        == f"https://{Config.S3_BUCKET}.s3.{Config.AWS_REGION}.amazonaws.com"
    )

    # Test that all paths use the S3 prefix
    paths = [
        Config.RATINGS_PATH,
        Config.BASICS_PATH,
        Config.CREW_PATH,
        Config.EPISODE_PATH,
        Config.NAMES_PATH,
        Config.PRINCIPALS_PATH,
        Config.AKAS_PATH,
    ]

    for path in paths:
        assert path.startswith(Config.S3_PREFIX), f"Path should use S3 prefix: {path}"
        assert path.endswith(".tsv"), f"Path should end with .tsv: {path}"


def test_ratings_schema_with_data(spark, temp_dir):
    """Test that ratings schema matches actual data structure"""
    try:
        # Download the file
        local_path = os.path.join(temp_dir, "title.ratings.tsv")
        download_file(Config.RATINGS_PATH, local_path)

        # Read with Spark
        df = spark.read.csv(
            local_path, header=True, sep="\t", schema=Config.RATINGS_SCHEMA
        )
        # Only fetch one row to verify schema
        df.limit(1).collect()
    except Exception as e:
        pytest.fail(f"Failed to read ratings data with schema: {str(e)}")


def test_min_votes_constant():
    """Test that MIN_VOTES is properly set"""
    assert isinstance(Config.MIN_VOTES, int), "MIN_VOTES should be an integer"
    assert Config.MIN_VOTES == 500, "MIN_VOTES should be 500"
    assert Config.MIN_VOTES > 0, "MIN_VOTES should be positive"


def test_spark_configs():
    """Test Spark configuration settings"""
    configs = Config.SPARK_CONFIGS

    # Check required configurations
    assert "spark.sql.streaming.schemaInference" in configs
    assert "spark.sql.streaming.checkpointLocation" in configs

    # Check checkpoint location matches
    assert (
        configs["spark.sql.streaming.checkpointLocation"] == Config.CHECKPOINT_LOCATION
    )


def test_output_modes():
    """Test output mode configurations"""
    modes = Config.OUTPUT_MODES

    # Check all required modes are present
    assert "console" in modes
    assert "memory" in modes
    assert "file" in modes

    # Check mode values are valid
    valid_modes = {"complete", "append", "update"}
    for mode in modes.values():
        assert mode in valid_modes, f"Invalid output mode: {mode}"


def test_stream_configuration():
    """Test streaming configuration settings"""
    assert Config.STREAM_TRIGGER_INTERVAL == "30 seconds"
    assert Config.CHECKPOINT_LOCATION == "./checkpoints"
