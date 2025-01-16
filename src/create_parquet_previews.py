import os
import pandas as pd


def create_parquet_previews():
    """Create previews of Parquet files in the data directory"""
    # List of Parquet files to create previews for
    parquet_files = [
        "title.ratings.parquet",
        "title.basics.parquet",
        "title.crew.parquet",
        "title.episode.parquet",
        "name.basics.parquet",
        "title.principals.parquet",
        "title.akas.parquet",
    ]

    for filename in parquet_files:
        print(f"Processing {filename}...")

        # Path to the local Parquet file
        parquet_path = os.path.join("data", filename)

        # Read Parquet file using pandas
        print(f"Reading {filename}")
        df = pd.read_parquet(parquet_path)

        # Create a preview of the first 10 rows
        preview_df = df.head(10)

        # Write the preview as a new Parquet file
        preview_parquet_path = os.path.join(
            "data", filename.replace(".parquet", "_preview.parquet")
        )
        print(f"Writing {preview_parquet_path}")
        preview_df.to_parquet(preview_parquet_path, index=False)

        print(f"Successfully created preview for {filename}\n")


if __name__ == "__main__":
    create_parquet_previews()
