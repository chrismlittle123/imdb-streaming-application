# Exercise Task

## Submission Requirements
 
Use Python and provide testing evidence.
 
## Data Source
    
Sources:

- data/name.basics.tsv: 

nconst	primaryName	birthYear	deathYear	primaryProfession	knownForTitles

- data/title.akas.tsv

tconst	titleType	primaryTitle	originalTitle	isAdult	startYear	endYear	runtimeMinutes	genres

- data/title.basics.tsv

tconst	titleType	primaryTitle	originalTitle	isAdult	startYear	endYear	runtimeMinutes	genres

- data/title.crew.tsv

tconst	directors	writers

- data/title.episode.tsv

tconst	parentTconst	seasonNumber	episodeNumber

- data/title.principals.tsv

tconst	ordering		nconst	category	job	characters

- data/title.ratings.tsv

tconst	averageRating	numVotes


## Problem Statement
 
Your task is to write a streaming application using Apache Spark that can answer the following questions using the imdb data set.
 
1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
(numVotes/averageNumberOfVotes) * averageRating
 
2. For these 10 movies, list the persons who are most often credited and list the
different titles of the 10 movies.
 
The application should:
-          be runnable on a local machine
-          have documentation on how to execute
-          be reproducible

