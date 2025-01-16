#!/usr/bin/env python3

import os
import sys

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession
from config import Config
from data_loader import initialize_data


def main():
    """Initialize the data by downloading TSV files and converting to Parquet"""

    # Create Spark session with proper configuration
    spark = SparkSession.builder.appName("IMDB Data Initialization").master("local[*]")

    # Add all configurations from Config
    for key, value in Config.SPARK_CONFIGS.items():
        spark = spark.config(key, value)

    # Create the session
    spark = spark.getOrCreate()

    try:
        print("Starting data initialization...")
        dataframes = initialize_data(spark)
        print("\nData initialization complete! Created the following Parquet files:")
        for filename in dataframes.keys():
            base = os.path.splitext(filename)[0]
            print(f"- {base}.parquet")
    except Exception as e:
        print(f"Error during data initialization: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
