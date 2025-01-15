# Implementation Plan for IMDB Data Analysis

## Overview
This plan outlines the step-by-step process to create a Spark streaming application that analyzes IMDB datasets to:
1. Find top 10 movies based on a weighted rating formula
2. Analyze credits and titles for these movies

## Prerequisites
1. Software Requirements:
   - Python 3.8+
   - Apache Spark 3.x
   - Required Python packages:
     - pyspark
     - pandas
     - pytest (for testing)

2. Data Requirements:
   - Ensure all IMDB TSV files are present in the `data/` directory:
     - name.basics.tsv
     - title.basics.tsv
     - title.crew.tsv
     - title.episode.tsv
     - title.principals.tsv
     - title.ratings.tsv

## Project Structure
```
imdb-analysis/
├── data/                     # IMDB TSV files
├── src/                      # Source code
│   ├── __init__.py
│   ├── config.py            # Configuration and constants
│   ├── spark_session.py     # Spark session management
│   ├── data_loader.py       # Data loading functions
│   └── movie_analyzer.py    # Core analysis logic
├── tests/                   # Test files
│   ├── __init__.py
│   ├── test_data_loader.py
│   └── test_movie_analyzer.py
├── requirements.txt         # Project dependencies
├── main.py                 # Application entry point
└── README.md              # Project documentation
```

## Implementation Steps

### Phase 1: Project Setup
1. Create the project structure as outlined above
2. Create requirements.txt with necessary dependencies
3. Set up a virtual environment
4. Create initial README.md with basic project information

### Phase 2: Core Components
1. Create config.py:
   - Define file paths
   - Define constants (e.g., MIN_VOTES = 500)
   - Define schema definitions for each TSV file

2. Create spark_session.py:
   - Implement SparkSession creation
   - Add configuration settings
   - Include cleanup handling 

## Detailed Implementation Guide

### Phase 8: Data Processing Pipeline Details

1. Ratings Analysis Pipeline:
   ```python
   def process_ratings():
       """
       Steps:
       1. Load ratings stream from title.ratings.tsv
       2. Calculate averageNumberOfVotes across all movies
       3. Filter for movies with numVotes >= 500
       4. Calculate weighted rating:
          (numVotes/averageNumberOfVotes) * averageRating
       5. Select top 10 movies by weighted rating
       """
   ```

2. Movie Details Pipeline:
   ```python
   def process_movie_details():
       """
       Steps:
       1. Load title.basics.tsv
       2. Filter where:
          - titleType = 'movie'
          - tconst matches top 10 from ratings
       3. Select required fields:
          - tconst
          - primaryTitle
          - originalTitle
          - startYear
          - genres
       """
   ```

3. Credits Analysis Pipeline:
   ```python
   def process_credits():
       """
       Steps:
       1. Load title.principals.tsv
       2. Join with name.basics.tsv on nconst
       3. Filter for top 10 movie tconst
       4. Group by person and count credits
       5. Rank by credit count
       """
   ```

### Phase 9: Streaming Implementation Details

1. Configure Streaming Source:
   ```python
   def setup_stream():
       return spark.readStream \
           .format("csv") \
           .option("sep", "\t") \
           .option("header", "true") \
           .schema(ratings_schema) \
           .load("data/title.ratings.tsv")
   ```

2. Define Processing Windows:
   ```python
   def configure_windows():
       """
       Set up streaming windows:
       - Window size: 1 minute
       - Slide interval: 30 seconds
       - Output mode: complete
       - Checkpoint location: ./checkpoints
       """
   ```

3. Output Handling:
   ```python
   def handle_output():
       """
       Define output sinks:
       1. Console output for debugging
       2. Memory sink for querying
       3. File sink for persistence
       """
   ```

### Phase 10: Specific Data Schemas

1. Ratings Schema:
   ```python
   ratings_schema = StructType([
       StructField("tconst", StringType(), False),
       StructField("averageRating", FloatType(), False),
       StructField("numVotes", IntegerType(), False)
   ])
   ```

2. Movies Schema:
   ```python
   movies_schema = StructType([
       StructField("tconst", StringType(), False),
       StructField("titleType", StringType(), False),
       StructField("primaryTitle", StringType(), False),
       StructField("originalTitle", StringType(), False),
       StructField("isAdult", BooleanType(), False),
       StructField("startYear", IntegerType(), True),
       StructField("endYear", IntegerType(), True),
       StructField("runtimeMinutes", IntegerType(), True),
       StructField("genres", StringType(), True)
   ])
   ```

3. Credits Schema:
   ```python
   principals_schema = StructType([
       StructField("tconst", StringType(), False),
       StructField("ordering", IntegerType(), False),
       StructField("nconst", StringType(), False),
       StructField("category", StringType(), True),
       StructField("job", StringType(), True),
       StructField("characters", StringType(), True)
   ])
   ```

### Phase 11: Performance Optimization

1. Caching Strategy:
   ```python
   def optimize_performance():
       """
       Cache frequently used DataFrames:
       - Movie metadata (small, frequently joined)
       - Name basics (medium, frequently joined)
       - Top 10 movies (very small, frequently accessed)
       """
   ```

2. Partition Strategy:
   ```python
   def configure_partitioning():
       """
       Optimize partitioning:
       - Ratings: by numVotes ranges
       - Movies: by year
       - Credits: by tconst
       """
   ```

Would you like me to continue with more specific details about any of these sections or move on to the next phase? 