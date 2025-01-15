import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from src.config import Config


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


@pytest.fixture(scope="module", autouse=True)
def cleanup_spark():
    """Clean up Spark session after tests"""
    yield
    SparkSession.builder.getOrCreate().stop()


def test_file_paths_exist():
    """Test that all configured file paths exist"""
    paths = [
        Config.RATINGS_PATH,  # title.ratings.tsv
        Config.BASICS_PATH,  # title.basics.tsv
        Config.NAMES_PATH,  # name.basics.tsv
    ]

    for path in paths:
        assert os.path.exists(path), f"File not found: {path}"


def test_schema_structures():
    """Test that all schemas are properly structured StructType objects"""
    schemas = [
        Config.RATINGS_SCHEMA,
        Config.MOVIES_SCHEMA,
        Config.NAMES_SCHEMA,
        Config.CREW_SCHEMA,
        Config.EPISODE_SCHEMA,
        Config.PRINCIPALS_SCHEMA,
        Config.AKAS_SCHEMA,
    ]

    for schema in schemas:
        assert isinstance(schema, StructType), "Schema is not a StructType"
        assert len(schema.fields) > 0, "Schema has no fields"


def test_ratings_schema_with_data(spark):
    """Test that ratings schema matches actual data structure"""
    try:
        df = spark.read.csv(
            Config.RATINGS_PATH, header=True, sep="\t", schema=Config.RATINGS_SCHEMA
        )
        # If schema doesn't match, this will raise an exception
        df.count()
    except Exception as e:
        pytest.fail(f"Failed to read ratings data with schema: {str(e)}")


def test_movies_schema_with_data(spark):
    """Test that movies schema matches actual data structure"""
    try:
        df = spark.read.csv(
            Config.BASICS_PATH, header=True, sep="\t", schema=Config.MOVIES_SCHEMA
        )
        df.count()
    except Exception as e:
        pytest.fail(f"Failed to read movies data with schema: {str(e)}")


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
