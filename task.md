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

Convert tsv files to parquet

Stream/batch load title.ratings and calculate the metric, take top 10.
 
2. For these 10 movies, list the persons who are most often credited and list the different titles of the 10 movies.

Use title.basics as a lookup table to get the primary title and original title of each film in the top 10. Lookup the titles dynamically the merge. Make sure to partition the title.basics table.

Also, create another table from name.basics which has title Id and the names of person (in a list format), essentially restructuring the data in that table. Use this table in this new structure as a lookup table for names associated with that film, in the same way that the lookup table title.basics was created.

Use the ids if the 10 movies and do 
 
The application should:
- Be runnable on a local machine
- Have documentation on how to execute
- Be reproducible


Use the following as a guide:

- Use Apache Spark configured with S3, the datasets are publicly available
- Read data in batches, read the file in chunks of 100 from S3
- Use limit and offset-like behavior to simulate batch processing
- Create a process function to handle each batch, calculating the desired metrics
- Remember to repartition the data
- Use pre-defined schemas
