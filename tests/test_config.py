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
