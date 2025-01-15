# Exercise Task

## Submission Requirements
 
Use Python and provide testing evidence.
 
## Data Source
    
The following datasets from IMDB are used in TSV (tab-separated values) format in this S3 bucket: imdb-data-495700631743

- **name.basics.tsv**
  - nconst (string) - Alphanumeric unique identifier for names
  - primaryName (string) - Name of the person
  - birthYear (integer) - Birth year in YYYY format
  - deathYear (integer) - Death year in YYYY format, or '\\N' if not applicable
  - primaryProfession (array) - Comma-separated list of primary professions
  - knownForTitles (array) - Comma-separated list of title IDs the person is known for

- **title.basics.tsv**
  - tconst (string) - Alphanumeric unique identifier for titles
  - titleType (string) - Type/format of the title (e.g., movie, short, series)
  - primaryTitle (string) - Popular title used for the work
  - originalTitle (string) - Original title in original language
  - isAdult (boolean) - 0: non-adult title; 1: adult title
  - startYear (integer) - Release year of title
  - endYear (integer) - End year for TV Series, '\\N' for other title types
  - runtimeMinutes (integer) - Primary runtime in minutes
  - genres (array) - Comma-separated list of genres

- **title.crew.tsv**
  - tconst (string) - Alphanumeric unique identifier for titles
  - directors (array) - Comma-separated list of nconst identifiers for directors
  - writers (array) - Comma-separated list of nconst identifiers for writers

- **title.episode.tsv**
  - tconst (string) - Alphanumeric identifier for this episode
  - parentTconst (string) - Alphanumeric identifier for the parent TV series
  - seasonNumber (integer) - Season number the episode belongs to
  - episodeNumber (integer) - Episode number within the season

- **title.principals.tsv**
  - tconst (string) - Alphanumeric unique identifier for titles
  - ordering (integer) - Order of importance in the title
  - nconst (string) - Alphanumeric unique identifier for names
  - category (string) - Category of job (e.g., actor, director)
  - job (string) - Specific job title if applicable
  - characters (string) - Name of character played if applicable

- **title.ratings.tsv**
  - tconst (string) - Alphanumeric unique identifier for titles
  - averageRating (float) - Weighted average of all user ratings
  - numVotes (integer) - Number of votes the title has received

## Problem Statement
 
Your task is to write a streaming application using Apache Spark that can answer the following questions using the IMDB dataset:
 
1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
   ```
   (numVotes/averageNumberOfVotes) * averageRating
   ```
 
2. For these 10 movies, list the persons who are most often credited and list the different titles of the 10 movies.
 
The application should:
- Be runnable on a local machine
- Have documentation on how to execute
- Be reproducible

