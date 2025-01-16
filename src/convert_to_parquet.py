import os
import pandas as pd
from config import Config


def get_schema_for_file(filename):
    """Get the appropriate schema based on the filename"""
    schema_mapping = {
        "title.ratings.tsv": Config.RATINGS_SCHEMA,
        "title.basics.tsv": Config.MOVIES_SCHEMA,
        "title.crew.tsv": Config.CREW_SCHEMA,
        "title.episode.tsv": Config.EPISODE_SCHEMA,
        "name.basics.tsv": Config.NAMES_SCHEMA,
        "title.principals.tsv": Config.PRINCIPALS_SCHEMA,
        "title.akas.tsv": Config.AKAS_SCHEMA,
    }
    return schema_mapping.get(filename)


def convert_tsv_to_parquet():
    """Convert all TSV files to Parquet format"""
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # List of files to convert
    files = [
        "title.ratings.tsv",
        "title.basics.tsv",
        "title.crew.tsv",
        "title.episode.tsv",
        "name.basics.tsv",
        "title.principals.tsv",
        "title.akas.tsv",
    ]

    for filename in files:
        print(f"Processing {filename}...")

        # Path to the local TSV file
        tsv_path = os.path.join("data", filename)

        # Get schema for the file
        schema = get_schema_for_file(filename)
        if not schema:
            print(f"Warning: No schema found for {filename}, skipping...")
            continue

        # Read TSV file using pandas
        print(f"Reading {filename}")
        df = pd.read_csv(
            tsv_path, sep="\t", dtype={field.name: str for field in schema.fields}
        )

        # Write as Parquet
        parquet_path = os.path.join("data", filename.replace(".tsv", ".parquet"))
        print(f"Writing {parquet_path}")
        df.to_parquet(parquet_path, index=False)

        # Remove original TSV file
        os.remove(tsv_path)
        print(f"Removed {tsv_path}")
        print(f"Successfully converted {filename} to Parquet format\n")


if __name__ == "__main__":
    convert_tsv_to_parquet()
