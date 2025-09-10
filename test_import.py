from file_to_postgres import DataImporter
import pandas as pd

# Initialize the importer
importer = DataImporter()

# List all test files and their target tables
test_cases = [
    ("test_files/fish_test.csv", "fish"),
    ("test_files/oceanography_test.csv", "oceanography"),
    ("test_files/edna_test.csv", "edna"),
]



for file_path, table_name in test_cases:
    print(f"\n{'='*50}")
    print(f"Importing data from: {file_path} into table: {table_name}")
    print(f"{'='*50}")
    # Read the file
    df = importer.read_file(file_path)
    print("\nData preview:")
    print(df.head())
    # Import to database
    success = importer.import_to_db(df, table_name)
    if success:
        print(f"\n✅ Successfully imported data from {file_path} into {table_name}")
    else:
        print(f"\n❌ Failed to import data from {file_path} into {table_name}")
    print(f"{'='*50}\n")

importer.close()