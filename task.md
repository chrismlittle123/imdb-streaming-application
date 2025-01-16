# Exercise Task

## Submission Requirements
 
Use Python and provide testing evidence.
 
## Data Source
    
The following datasets from IMDB are used in parquet format in this S3 bucket: imdb-data-495700631743

- **name.basics**
  - nconst (string) - Alphanumeric unique identifier for names
  - primaryName (string) - Name of the person
  - birthYear (integer) - Birth year in YYYY format
  - deathYear (integer) - Death year in YYYY format, or '\\N' if not applicable
  - primaryProfession (array) - Comma-separated list of primary professions
  - knownForTitles (array) - Comma-separated list of title IDs the person is known for

- **title.basics**
  - tconst (string) - Alphanumeric unique identifier for titles
  - titleType (string) - Type/format of the title (e.g., movie, short, tvSeries)
  - primaryTitle (string) - The more popular title / the title used by the filmmakers on promotional materials at the point of release
  - originalTitle (string) - Original title, in the original language
  - isAdult (boolean) - 0: non-adult title; 1: adult title
  - startYear (integer) - Release year of a title in YYYY format
  - endYear (integer) - End year of a title in YYYY format
  - runtimeMinutes (integer) - Primary runtime of the title, in minutes
  - genres (array) - Comma-separated list of genres associated with the title

- **title.ratings**
  - tconst (string) - Alphanumeric unique identifier for titles
  - averageRating (float) - Weighted average of all user ratings
  - numVotes (integer) - Number of votes the title has received

## Problem Statement
 
Your task is to write a streaming application using Apache Spark that can answer the following questions using the IMDB dataset:
 
1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
   ```
   (numVotes/averageNumberOfVotes) * averageRating
   ```


Batch load title.ratings with batch size of 100 and calculate the metric, take top 10.
 
2. For these 10 movies, list the persons who are most often credited and list the different titles of the 10 movies.

Use title.basics as a lookup table to get the primary title and original title of each film in the top 10. Lookup the titles dynamically the merge. Make sure to partition the title.basics table.

Also, create another table from name.basics which has title Id and the names of person (in a list format), essentially restructuring the data in that table. Use this table in this new structure as a lookup table for names associated with that film, in the same way that the lookup table title.basics was created.

Use the ids if the 10 movies and do 
 
The application should:
- Be runnable on a local machine
- Have documentation on how to execute
- Be reproducible
