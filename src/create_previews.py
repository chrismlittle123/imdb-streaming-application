import os
import pandas as pd


def create_tsv_previews():
    """Create previews of TSV files in the data directory"""
    # List of TSV files to create previews for
    tsv_files = [
        "title.ratings.tsv",
        "title.basics.tsv",
        "name.basics.tsv",
    ]

    for filename in tsv_files:
        print(f"Processing {filename}...")

        # Path to the local TSV file
        tsv_path = os.path.join("data", filename)

        # Read TSV file using pandas
        print(f"Reading {filename}")
        df = pd.read_csv(tsv_path, sep='\t')

        # Create a preview of the first 10 rows
        preview_df = df.head(10)

        # Write the preview as a new TSV file
        preview_tsv_path = os.path.join(
            "data", filename.replace(".tsv", "_preview.tsv")
        )
        print(f"Writing {preview_tsv_path}")
        preview_df.to_csv(preview_tsv_path, sep='\t', index=False)

        print(f"Successfully created preview for {filename}\n")


if __name__ == "__main__":
    create_tsv_previews()
