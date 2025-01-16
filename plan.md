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
     - pytest (for testing)


## Project Structure
```
imdb-analysis/
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

1. Create Dockerfile and docker-compose to run Spark in a Docker container. 

2. Create config.py:
   - Define file paths
   - Define constants (e.g., MIN_VOTES = 500)
   - Define schema definitions for each TSV file

3. Test config.py

4. Create spark_session.py:

   - Implement SparkSession creation
   - Add configuration settings
   - Include cleanup handling 


### Phase 3: Data Processing Pipeline Details

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

### Phase 4: Streaming Implementation Details

1. Configure Streaming Source:
   - Use sources defined in config.py
   - Batch stream in chunks of 100


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


4. Partition Strategy:
   ```python
   def configure_partitioning():
       """
       Optimize partitioning:
       - Ratings: by numVotes ranges
       - Movies: by year
       - Credits: by tconst
       """
   ```