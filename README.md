# imdb-streaming-application

A Spark streaming application that analyzes IMDB datasets to identify top-rated movies and analyze their credits.

### Run container

docker-compose build
docker-compose up

### Run tests

docker run --network host --add-host spark:127.0.0.1 -v "$(pwd)/src:/app/src" -v "$(pwd)/tests:/app/tests" chrismlittle123/imdb-streaming-app:latest python3 -m pytest tests/ -v

### Documentation

## Project Overview

This application processes IMDB datasets using Apache Spark to perform streaming analysis of movie ratings and credits. The main objectives are:

1. Identify the top 10 movies based on a weighted ranking system
2. Analyze credits and alternative titles for these top movies

## Data Sources

The application uses three primary IMDB datasets in TSV format:

1. **title.ratings.tsv**
   - Contains movie ratings data
   - Fields: tconst (ID), averageRating, numVotes
   - Used for calculating movie rankings

2. **title.basics.tsv**
   - Contains movie metadata
   - Fields: tconst, titleType, primaryTitle, originalTitle, startYear, etc.
   - Used as a lookup table for movie titles

3. **name.basics.tsv**
   - Contains person/credit information
   - Fields: nconst (ID), primaryName, birthYear, knownForTitles, etc.
   - Used for analyzing movie credits

## Architecture

### Stream Processor (`src/stream_processor.py`)

The core component is the `StreamProcessor` class, which handles:

1. **Data Loading**
   - Downloads TSV files from S3 if not available locally
   - Reads data using appropriate schemas
   - Handles data partitioning for efficient processing

2. **Movie Ranking**
   - Calculates average number of votes across all movies
   - Applies the ranking formula: `(numVotes/averageNumberOfVotes) * averageRating`
   - Filters movies with at least 500 votes
   - Identifies top 10 movies by ranking

3. **Credits Analysis**
   - Processes the `name.basics` dataset
   - Links credits to top-rated movies
   - Identifies frequently credited persons

### Key Features

1. **Batch Processing**
   - Processes ratings data in configurable batch sizes (default: 100)
   - Maintains state between batches for accurate rankings

2. **Dynamic Lookups**
   - Uses `title.basics` as a lookup table for movie titles
   - Partitions lookup tables for efficient joins
   - Performs dynamic merging of movie metadata

3. **Data Transformation**
   - Restructures the `name.basics` data for efficient credit lookup
   - Handles multiple title formats (primary and original titles)
   - Manages data type conversions and null values

## Implementation Details

### Configuration (`src/config.py`)

- Defines data schemas for all TSV files
- Sets configuration parameters:
  - Minimum vote threshold (500)
  - Batch size (100)
  - S3 paths and credentials
  - Spark configuration settings

### Spark Session Management

- Creates and configures Spark sessions with optimized settings
- Handles memory allocation for driver and executor
- Manages streaming configurations and backpressure

### Data Processing Flow

1. **Initialization**
   - Create Spark session
   - Configure memory and partitioning
   - Initialize data directories

2. **Data Loading**
   - Download TSV files if needed
   - Read data with appropriate schemas
   - Apply initial filtering (e.g., movie type)

3. **Ranking Calculation**
   - Calculate average votes
   - Apply ranking formula
   - Filter by minimum votes
   - Sort and select top 10

4. **Credits Analysis**
   - Join with title.basics for movie information
   - Process name.basics for credits
   - Aggregate and analyze credit frequency

### Error Handling

- Graceful handling of missing files
- Download retry logic for S3 access
- Proper cleanup of Spark sessions
- Validation of data schemas and types

## Testing

The application includes comprehensive tests:

1. **Configuration Tests**
   - Validate S3 paths
   - Test schema definitions
   - Verify configuration constants

2. **Stream Processor Tests**
   - Test initialization
   - Verify average vote calculation
   - Validate ranking formula
   - Test minimum votes filtering

3. **Integration Tests**
   - End-to-end processing tests
   - Data transformation validation
   - Batch processing verification

## Docker Implementation

The application is containerized using Docker:

1. **Base Image**
   - Uses Apache Spark base image
   - Includes Python 3.8 runtime
   - Configures necessary dependencies

2. **Volume Mounting**
   - Maps source code and tests
   - Persists downloaded data
   - Manages test artifacts

3. **Network Configuration**
   - Configures host networking
   - Sets up Spark driver binding
   - Manages container communication

## Usage

1. **Build and Run**
   ```bash
   docker-compose build
   docker-compose up
   ```

2. **Run Tests**
   ```bash
   docker run --network host --add-host spark:127.0.0.1 -v "$(pwd)/src:/app/src" -v "$(pwd)/tests:/app/tests" chrismlittle123/imdb-streaming-app:latest python3 -m pytest tests/ -v
   ```

3. **View Results**
   - Top 10 movies are displayed with rankings
   - Credit analysis shows frequently appearing persons
   - Alternative titles are listed for each movie

