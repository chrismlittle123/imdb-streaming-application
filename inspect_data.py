from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("IMDB Data Inspection").getOrCreate()

# Read and inspect each TSV file
files = {
    "name.basics": "data/name.basics.tsv",
    "title.akas": "data/title.akas.tsv",
    "title.basics": "data/title.basics.tsv",
    "title.crew": "data/title.crew.tsv",
    "title.episode": "data/title.episode.tsv",
    "title.principals": "data/title.principals.tsv",
    "title.ratings": "data/title.ratings.tsv",
}

for name, path in files.items():
    print(f"\nInspecting {name}:")
    df = pd.read_csv(path, sep="\t", nrows=5)
    print("\nColumns:", df.columns.tolist())
    print("\nSample data:")
    print(df.head(2))
